// Web Worker: 异步数据聚合处理
// 用于处理大量数据，避免阻塞主线程

// 监听主线程消息
self.addEventListener('message', function(e) {
    const { type, data, options } = e.data;
    
    switch(type) {
        case 'aggregate':
            aggregateData(data, options);
            break;
        case 'filter':
            filterData(data, options);
            break;
        case 'statistics':
            calculateStatistics(data, options);
            break;
        default:
            self.postMessage({ error: 'Unknown command: ' + type });
    }
});

// 数据聚合：按时间窗口聚合数据
function aggregateData(data, options) {
    const { timeColumn, columns, windowSize = 1000, aggregation = 'mean' } = options;
    
    try {
        // 识别时间列
        let timeIndex = -1;
        if (timeColumn) {
            timeIndex = data.columns.indexOf(timeColumn);
        } else {
            // 自动查找时间列
            const timePatterns = ['Date', 'Time', '时间', '日期', 'timestamp'];
            for (let i = 0; i < data.columns.length; i++) {
                if (timePatterns.some(pattern => 
                    data.columns[i].toLowerCase().includes(pattern.toLowerCase())
                )) {
                    timeIndex = i;
                    break;
                }
            }
        }
        
        if (timeIndex === -1) {
            // 如果没有时间列，使用索引作为时间
            timeIndex = 0;
        }
        
        // 解析时间数据
        const timeData = data.rows.map(row => {
            const timeVal = row[timeIndex];
            if (typeof timeVal === 'number') {
                return timeVal;
            }
            // 尝试解析时间字符串
            const parsed = new Date(timeVal);
            return isNaN(parsed.getTime()) ? null : parsed.getTime();
        });
        
        // 过滤有效数据
        const validIndices = [];
        for (let i = 0; i < timeData.length; i++) {
            if (timeData[i] !== null && !isNaN(timeData[i])) {
                validIndices.push(i);
            }
        }
        
        // 按时间排序
        validIndices.sort((a, b) => timeData[a] - timeData[b]);
        
        // 聚合数据
        const aggregated = [];
        const numWindows = Math.ceil(validIndices.length / windowSize);
        
        for (let w = 0; w < numWindows; w++) {
            const startIdx = w * windowSize;
            const endIdx = Math.min(startIdx + windowSize, validIndices.length);
            const windowIndices = validIndices.slice(startIdx, endIdx);
            
            if (windowIndices.length === 0) continue;
            
            const windowData = {
                time: timeData[windowIndices[Math.floor(windowIndices.length / 2)]], // 使用中间时间
                values: {}
            };
            
            // 对每个列进行聚合
            columns.forEach(col => {
                const colIndex = data.columns.indexOf(col);
                if (colIndex === -1) return;
                
                const values = windowIndices
                    .map(idx => data.rows[idx][colIndex])
                    .filter(v => v !== null && v !== undefined && !isNaN(v));
                
                if (values.length > 0) {
                    switch(aggregation) {
                        case 'mean':
                            windowData.values[col] = values.reduce((a, b) => a + b, 0) / values.length;
                            break;
                        case 'sum':
                            windowData.values[col] = values.reduce((a, b) => a + b, 0);
                            break;
                        case 'max':
                            windowData.values[col] = Math.max(...values);
                            break;
                        case 'min':
                            windowData.values[col] = Math.min(...values);
                            break;
                        case 'median':
                            const sorted = [...values].sort((a, b) => a - b);
                            windowData.values[col] = sorted[Math.floor(sorted.length / 2)];
                            break;
                        default:
                            windowData.values[col] = values.reduce((a, b) => a + b, 0) / values.length;
                    }
                } else {
                    windowData.values[col] = null;
                }
            });
            
            aggregated.push(windowData);
            
            // 定期发送进度
            if (w % 10 === 0) {
                self.postMessage({
                    type: 'progress',
                    progress: Math.round((w / numWindows) * 100)
                });
            }
        }
        
        self.postMessage({
            type: 'aggregated',
            data: aggregated,
            columns: columns,
            timeColumn: timeColumn || 'index'
        });
        
    } catch (error) {
        self.postMessage({
            type: 'error',
            error: error.message,
            stack: error.stack
        });
    }
}

// 数据过滤
function filterData(data, options) {
    const { columns, filters } = options;
    
    try {
        const filtered = data.rows.filter(row => {
            return filters.every(filter => {
                const colIndex = data.columns.indexOf(filter.column);
                if (colIndex === -1) return true;
                
                const value = row[colIndex];
                switch(filter.operator) {
                    case '>':
                        return value > filter.value;
                    case '<':
                        return value < filter.value;
                    case '>=':
                        return value >= filter.value;
                    case '<=':
                        return value <= filter.value;
                    case '==':
                        return value == filter.value;
                    case '!=':
                        return value != filter.value;
                    default:
                        return true;
                }
            });
        });
        
        self.postMessage({
            type: 'filtered',
            data: {
                columns: data.columns,
                rows: filtered
            }
        });
        
    } catch (error) {
        self.postMessage({
            type: 'error',
            error: error.message
        });
    }
}

// 计算统计信息
function calculateStatistics(data, options) {
    const { columns } = options;
    
    try {
        const stats = {};
        
        columns.forEach(col => {
            const colIndex = data.columns.indexOf(col);
            if (colIndex === -1) return;
            
            const values = data.rows
                .map(row => row[colIndex])
                .filter(v => v !== null && v !== undefined && !isNaN(v) && typeof v === 'number');
            
            if (values.length === 0) {
                stats[col] = null;
                return;
            }
            
            const sorted = [...values].sort((a, b) => a - b);
            const sum = values.reduce((a, b) => a + b, 0);
            const mean = sum / values.length;
            const variance = values.reduce((acc, v) => acc + Math.pow(v - mean, 2), 0) / values.length;
            const std = Math.sqrt(variance);
            
            stats[col] = {
                count: values.length,
                mean: mean,
                std: std,
                min: sorted[0],
                max: sorted[sorted.length - 1],
                median: sorted[Math.floor(sorted.length / 2)],
                q25: sorted[Math.floor(sorted.length * 0.25)],
                q75: sorted[Math.floor(sorted.length * 0.75)]
            };
        });
        
        self.postMessage({
            type: 'statistics',
            stats: stats
        });
        
    } catch (error) {
        self.postMessage({
            type: 'error',
            error: error.message
        });
    }
}

