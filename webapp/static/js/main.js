/**
 * Main JavaScript for PyMiniDB Web Interface
 */

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    const tooltips = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    tooltips.forEach(tooltip => {
        new bootstrap.Tooltip(tooltip);
    });
    
    // Initialize popovers
    const popovers = document.querySelectorAll('[data-bs-toggle="popover"]');
    popovers.forEach(popover => {
        new bootstrap.Popover(popover);
    });
    
    // Auto-refresh data tables every 30 seconds
    if (document.querySelector('.auto-refresh')) {
        setInterval(() => {
            const refreshBtn = document.querySelector('.refresh-data');
            if (refreshBtn) refreshBtn.click();
        }, 30000);
    }
    
    // SQL query editor enhancement
    const sqlEditor = document.getElementById('sqlEditor');
    if (sqlEditor) {
        // Add basic SQL syntax highlighting
        sqlEditor.addEventListener('input', function() {
            this.style.height = 'auto';
            this.style.height = (this.scrollHeight) + 'px';
        });
        
        // Tab key support
        sqlEditor.addEventListener('keydown', function(e) {
            if (e.key === 'Tab') {
                e.preventDefault();
                const start = this.selectionStart;
                const end = this.selectionEnd;
                
                // Insert tab character
                this.value = this.value.substring(0, start) + '    ' + this.value.substring(end);
                
                // Move cursor position
                this.selectionStart = this.selectionEnd = start + 4;
            }
        });
    }
    
    // Table row selection
    document.querySelectorAll('.table tbody tr').forEach(row => {
        row.addEventListener('click', function(e) {
            if (!e.target.matches('a, button, input, select')) {
                this.classList.toggle('table-active');
            }
        });
    });
    
    // Confirm deletions
    document.querySelectorAll('.confirm-delete').forEach(button => {
        button.addEventListener('click', function(e) {
            if (!confirm('Are you sure you want to delete this item?')) {
                e.preventDefault();
            }
        });
    });
    
    // Form validation
    document.querySelectorAll('form.needs-validation').forEach(form => {
        form.addEventListener('submit', function(e) {
            if (!form.checkValidity()) {
                e.preventDefault();
                e.stopPropagation();
            }
            form.classList.add('was-validated');
        });
    });
    
    // Copy to clipboard functionality
    document.querySelectorAll('.copy-to-clipboard').forEach(button => {
        button.addEventListener('click', function() {
            const text = this.dataset.copy || this.previousElementSibling.textContent;
            navigator.clipboard.writeText(text).then(() => {
                const original = this.innerHTML;
                this.innerHTML = '<i class="bi bi-check"></i> Copied!';
                setTimeout(() => {
                    this.innerHTML = original;
                }, 2000);
            });
        });
    });
    
    // Dynamic form field addition
    document.querySelectorAll('.add-field').forEach(button => {
        button.addEventListener('click', function() {
            const template = this.dataset.template;
            const container = document.getElementById(this.dataset.target);
            if (template && container) {
                const newField = document.createElement('div');
                newField.innerHTML = template;
                container.appendChild(newField);
                
                // Re-initialize tooltips on new elements
                new bootstrap.Tooltip(newField.querySelector('[data-bs-toggle="tooltip"]'));
            }
        });
    });
    
    // Auto-submit forms on change
    document.querySelectorAll('.auto-submit').forEach(select => {
        select.addEventListener('change', function() {
            this.form.submit();
        });
    });
    
    // Toggle visibility
    document.querySelectorAll('.toggle-visibility').forEach(button => {
        button.addEventListener('click', function() {
            const target = document.getElementById(this.dataset.target);
            if (target) {
                const isHidden = target.classList.contains('d-none');
                target.classList.toggle('d-none');
                this.innerHTML = isHidden ? 
                    this.dataset.showText || 'Hide' : 
                    this.dataset.hideText || 'Show';
            }
        });
    });
    
    // Smooth scrolling for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });
    
    // Initialize charts if Chart.js is available
    if (typeof Chart !== 'undefined') {
        initCharts();
    }
});

/**
 * Initialize dashboard charts
 */
function initCharts() {
    const ctx = document.getElementById('dashboardChart');
    if (ctx) {
        new Chart(ctx.getContext('2d'), {
            type: 'bar',
            data: {
                labels: ['Tables', 'Rows', 'Indexes', 'Queries'],
                datasets: [{
                    label: 'Database Metrics',
                    data: [12, 19, 3, 5],
                    backgroundColor: [
                        'rgba(13, 110, 253, 0.8)',
                        'rgba(25, 135, 84, 0.8)',
                        'rgba(255, 193, 7, 0.8)',
                        'rgba(220, 53, 69, 0.8)'
                    ],
                    borderColor: [
                        'rgb(13, 110, 253)',
                        'rgb(25, 135, 84)',
                        'rgb(255, 193, 7)',
                        'rgb(220, 53, 69)'
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }
}

/**
 * Show loading spinner
 */
function showLoading(button) {
    const original = button.innerHTML;
    button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Loading...';
    button.disabled = true;
    return original;
}

/**
 * Hide loading spinner
 */
function hideLoading(button, original) {
    button.innerHTML = original;
    button.disabled = false;
}

/**
 * Show notification
 */
function showNotification(message, type = 'info') {
    const container = document.getElementById('notifications') || createNotificationContainer();
    
    const alert = document.createElement('div');
    alert.className = `alert alert-${type} alert-dismissible fade show`;
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    container.appendChild(alert);
    
    // Auto remove after 5 seconds
    setTimeout(() => {
        if (alert.parentNode) {
            alert.remove();
        }
    }, 5000);
}

/**
 * Create notification container if it doesn't exist
 */
function createNotificationContainer() {
    const container = document.createElement('div');
    container.id = 'notifications';
    container.className = 'position-fixed top-0 end-0 p-3';
    container.style.zIndex = '9999';
    document.body.appendChild(container);
    return container;
}

/**
 * Format SQL query for display
 */
function formatSQL(sql) {
    const keywords = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 
                      'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP',
                      'INDEX', 'PRIMARY', 'KEY', 'UNIQUE', 'NOT', 'NULL',
                      'AND', 'OR', 'ORDER', 'BY', 'LIMIT', 'JOIN', 'ON',
                      'GROUP', 'HAVING', 'ASC', 'DESC', 'BETWEEN', 'LIKE',
                      'IN', 'IS', 'ALTER', 'ADD', 'COLUMN', 'MODIFY',
                      'RENAME', 'TO', 'TRUNCATE', 'BEGIN', 'COMMIT', 'ROLLBACK'];
    
    let formatted = sql;
    
    keywords.forEach(keyword => {
        const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
        formatted = formatted.replace(regex, `<span class="sql-keyword">${keyword}</span>`);
    });
    
    // Highlight strings
    formatted = formatted.replace(/'[^']*'/g, '<span class="sql-string">$&</span>');
    
    // Highlight numbers
    formatted = formatted.replace(/\b\d+\b/g, '<span class="sql-number">$&</span>');
    
    return formatted;
}

/**
 * Execute SQL query via AJAX
 */
function executeQuery(sql) {
    const button = event?.target || document.querySelector('#executeQuery');
    const original = showLoading(button);
    
    fetch('/api/query', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ sql: sql })
    })
    .then(response => response.json())
    .then(data => {
        hideLoading(button, original);
        displayQueryResults(data);
    })
    .catch(error => {
        hideLoading(button, original);
        showNotification('Error executing query: ' + error, 'danger');
    });
}

/**
 * Display query results
 */
function displayQueryResults(data) {
    const resultsDiv = document.getElementById('queryResults');
    if (!resultsDiv) return;
    
    if (data.success) {
        let html = `
            <div class="alert alert-success">
                <strong>Success!</strong> ${data.message}
                <br><small>Execution time: ${data.execution_time.toFixed(3)}s</small>
            </div>
        `;
        
        if (data.data && data.data.length > 0) {
            html += '<div class="table-responsive"><table class="table table-striped table-hover"><thead><tr>';
            
            // Header row
            Object.keys(data.data[0]).forEach(column => {
                html += `<th>${column}</th>`;
            });
            html += '</tr></thead><tbody>';
            
            // Data rows
            data.data.forEach(row => {
                html += '<tr>';
                Object.values(row).forEach(value => {
                    html += `<td>${value !== null ? value : '<span class="text-muted">NULL</span>'}</td>`;
                });
                html += '</tr>';
            });
            
            html += `</tbody></table></div>
                <div class="text-muted small mt-2">
                    ${data.data.length} row${data.data.length !== 1 ? 's' : ''} returned
                </div>`;
        }
        
        resultsDiv.innerHTML = html;
    } else {
        resultsDiv.innerHTML = `
            <div class="alert alert-danger">
                <strong>Error!</strong> ${data.error || data.message}
            </div>
        `;
    }
    
    // Scroll to results
    resultsDiv.scrollIntoView({ behavior: 'smooth' });
}

/**
 * Export table data as CSV
 */
function exportTableAsCSV(tableName) {
    fetch(`/api/table/${tableName}`)
        .then(response => response.json())
        .then(data => {
            if (data.success && data.data && data.data.length > 0) {
                const headers = Object.keys(data.data[0]);
                const csvRows = [
                    headers.join(','),
                    ...data.data.map(row => 
                        headers.map(header => {
                            const value = row[header];
                            // Escape quotes and wrap in quotes if contains comma or quotes
                            const escaped = ('' + value).replace(/"/g, '""');
                            return /[,"\n]/.test(escaped) ? `"${escaped}"` : escaped;
                        }).join(',')
                    )
                ];
                
                const csvString = csvRows.join('\n');
                const blob = new Blob([csvString], { type: 'text/csv' });
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `${tableName}_export.csv`;
                a.click();
                window.URL.revokeObjectURL(url);
                
                showNotification(`Table ${tableName} exported successfully!`, 'success');
            } else {
                showNotification('No data to export', 'warning');
            }
        })
        .catch(error => {
            showNotification('Error exporting data: ' + error, 'danger');
        });
}

/**
 * Import data from CSV
 */
function importDataFromCSV(tableName, file) {
    if (!file) return;
    
    const reader = new FileReader();
    reader.onload = function(e) {
        const csv = e.target.result;
        const rows = csv.split('\n');
        const headers = rows[0].split(',').map(h => h.trim());
        
        // Parse CSV rows
        const data = rows.slice(1)
            .filter(row => row.trim())
            .map(row => {
                const values = [];
                let inQuotes = false;
                let currentValue = '';
                
                for (let i = 0; i < row.length; i++) {
                    const char = row[i];
                    const nextChar = row[i + 1];
                    
                    if (char === '"' && nextChar === '"') {
                        currentValue += '"';
                        i++; // Skip next quote
                    } else if (char === '"') {
                        inQuotes = !inQuotes;
                    } else if (char === ',' && !inQuotes) {
                        values.push(currentValue);
                        currentValue = '';
                    } else {
                        currentValue += char;
                    }
                }
                values.push(currentValue);
                
                const rowData = {};
                headers.forEach((header, index) => {
                    rowData[header] = values[index] || '';
                });
                return rowData;
            });
        
        // Insert data
        data.forEach(row => {
            const columns = Object.keys(row).join(', ');
            const values = Object.values(row)
                .map(v => `'${v.replace(/'/g, "''")}'`)
                .join(', ');
            
            const sql = `INSERT INTO ${tableName} (${columns}) VALUES (${values})`;
            
            fetch('/api/query', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ sql: sql })
            })
            .then(response => response.json())
            .then(result => {
                if (!result.success) {
                    console.error('Error inserting row:', result.error);
                }
            });
        });
        
        showNotification(`Imported ${data.length} rows into ${tableName}`, 'success');
        setTimeout(() => location.reload(), 1000);
    };
    
    reader.readAsText(file);
}

// Make functions available globally
window.PyMiniDB = {
    showNotification,
    executeQuery,
    exportTableAsCSV,
    importDataFromCSV,
    formatSQL
};