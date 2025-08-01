// Global data storage
let jobData = null;
let statusChart = null;
let columnFilters = {};
let currentPage = 1;
let jobsPerPage = 50;

// Status colors - gray gradient progression with red for cancelled
const statusColors = {
    'Accepting applications': '#E2E8F0',    // Light gray - Active/Open
    'Applications under review': '#A0AEC0', // Medium light gray - In progress
    'Candidate selected': '#718096',       // Medium gray - Success
    'Job closed': '#4A5568',              // Dark gray - Closed
    'Job canceled': '#E53E3E'             // Red - Cancelled
};

// Define the logical order for statuses
const statusOrder = [
    'Accepting applications',
    'Applications under review', 
    'Candidate selected',
    'Job closed',
    'Job canceled'
];

// Load and process data
async function loadData() {
    try {
        // Show loading with size info
        document.getElementById('loading').innerHTML = 'Loading job status data...<br><span style="font-size: 0.9em; color: #a0aec0;">This may take a moment on first load</span>';
        
        // Try optimized version first, then minified, then regular
        let response = await fetch('job_status_data_optimized.json')
            .catch(() => fetch('job_status_data.min.json'))
            .catch(() => fetch('job_status_data.json'));
        jobData = await response.json();
        
        console.log('Data loaded successfully:', jobData);
        
        document.getElementById('loading').style.display = 'none';
        document.getElementById('content').style.display = 'block';
        
        updateSummaryStats();
        createStatusChart();
        populateCancelledJobsTable();
        setupColumnFilters();
        
        // Update last updated time and date range
        const startDate = new Date(jobData.summary.date_range.start).toLocaleDateString();
        const endDate = new Date(jobData.summary.date_range.end).toLocaleDateString();
        document.getElementById('lastUpdated').textContent = 
            `Data from ${startDate} to ${endDate} • Last updated: ${jobData.summary.generated_at}`;
        
    } catch (error) {
        console.error('Error loading data:', error);
        console.error('Stack trace:', error.stack);
        document.getElementById('loading').innerHTML = 
            'Error loading data. Please ensure job_status_data.json exists. Check browser console for details.';
    }
}

function updateSummaryStats() {
    const summary = jobData.summary;
    
    document.getElementById('totalJobs').textContent = summary.total_jobs.toLocaleString();
    document.getElementById('cancelledJobs').textContent = summary.cancelled_jobs_count.toLocaleString();
    document.getElementById('cancellationRate').textContent = `${summary.cancellation_rate}% cancellation rate`;
    
    const startDate = new Date(summary.date_range.start).toLocaleDateString();
    const endDate = new Date(summary.date_range.end).toLocaleDateString();
    document.getElementById('dateRange').textContent = `${startDate} - ${endDate}`;
}

function createStatusChart() {
    const ctx = document.getElementById('statusChart').getContext('2d');
    
    // Get current date and month start
    const today = new Date();
    const currentMonth = new Date(today.getFullYear(), today.getMonth(), 1);
    
    // Filter out the current incomplete month
    const filteredMonthlyData = jobData.monthly_status.filter(d => {
        const monthStart = new Date(d.month_start);
        return monthStart < currentMonth;
    });
    
    // Prepare data for horizontal bar chart
    const months = filteredMonthlyData.map(d => d.month_label);
    const datasets = [];
    
    // Use the logical order for statuses
    const allStatuses = statusOrder;
    
    // Create dataset for each status in order
    allStatuses.forEach(status => {
        const data = filteredMonthlyData.map(d => d[status] || 0);
        
        datasets.push({
            label: status,
            data: data,
            backgroundColor: statusColors[status],
            borderColor: 'transparent',
            borderWidth: 0,
            borderRadius: 4
        });
    });
    
    statusChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: months,
            datasets: datasets
        },
        options: {
            indexAxis: 'y', // This makes it horizontal
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    stacked: true,
                    title: {
                        display: true,
                        text: 'Number of Jobs',
                        color: '#4a5568',
                        font: {
                            size: 13,
                            weight: '600'
                        }
                    },
                    grid: {
                        color: '#e2e8f0',
                        borderColor: '#cbd5e0'
                    },
                    ticks: {
                        color: '#4a5568'
                    }
                },
                y: {
                    stacked: true,
                    title: {
                        display: true,
                        text: 'Month',
                        color: '#4a5568',
                        font: {
                            size: 13,
                            weight: '600'
                        }
                    },
                    grid: {
                        display: false
                    },
                    ticks: {
                        color: '#4a5568',
                        font: {
                            size: 12
                        }
                    }
                }
            },
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        color: '#4a5568',
                        font: {
                            size: 12,
                            weight: '500'
                        },
                        padding: 15,
                        usePointStyle: true,
                        pointStyle: 'rectRounded'
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        afterTitle: function(context) {
                            const index = context[0].dataIndex;
                            const monthData = filteredMonthlyData[index];
                            return `Total jobs: ${monthData.total_jobs}`;
                        }
                    }
                }
            }
        }
    });
}

function formatSalary(min, max) {
    if (!min && !max) return 'N/A';
    const minStr = min ? `$${min.toLocaleString()}` : '';
    const maxStr = max ? `$${max.toLocaleString()}` : '';
    if (min && max) return `${minStr} - ${maxStr}`;
    return minStr || maxStr;
}

function formatOpenings(openings) {
    if (!openings) return 'N/A';
    
    let badgeClass = 'openings-number';
    if (openings.toUpperCase() === 'MANY') badgeClass = 'openings-many';
    else if (openings.toUpperCase() === 'FEW') badgeClass = 'openings-few';
    
    return `<span class="openings-badge ${badgeClass}">${openings}</span>`;
}

function populateCancelledJobsTable(filteredJobs = null, page = 1) {
    const jobs = filteredJobs || jobData.cancelled_jobs;
    const tbody = document.getElementById('cancelledJobsBody');
    tbody.innerHTML = '';
    
    // Calculate pagination
    const totalJobs = jobs.length;
    const totalPages = Math.ceil(totalJobs / jobsPerPage);
    currentPage = Math.min(page, totalPages);
    
    const startIndex = (currentPage - 1) * jobsPerPage;
    const endIndex = Math.min(startIndex + jobsPerPage, totalJobs);
    const jobsToShow = jobs.slice(startIndex, endIndex);
    
    jobsToShow.forEach(job => {
        const row = document.createElement('tr');
        row.className = 'cancelled-row';
        
        const grade = job.min_grade && job.max_grade ? 
            (job.min_grade === job.max_grade ? `GS-${job.min_grade}` : `GS-${job.min_grade}-${job.max_grade}`) : 
            job.min_grade ? `GS-${job.min_grade}` : 
            job.max_grade ? `GS-${job.max_grade}` : 'N/A';
        
        row.innerHTML = `
            <td><a href="${job.usajobs_url}" target="_blank">${job.control_number}</a></td>
            <td>${job.position_title}</td>
            <td>${job.hiring_agency}</td>
            <td>${job.location || 'N/A'}</td>
            <td>${job.open_date}</td>
            <td>${job.close_date}</td>
            <td>${job.days_open !== null ? job.days_open : 'N/A'}</td>
            <td>${grade}</td>
            <td>${formatSalary(job.min_salary, job.max_salary)}</td>
            <td>${formatOpenings(job.total_openings)}</td>
            <td>${job.service_type || 'N/A'}</td>
        `;
        
        tbody.appendChild(row);
    });
    
    // Update pagination info and controls
    updatePaginationControls(totalJobs, totalPages, filteredJobs);
}


function setupColumnFilters() {
    // Add clear filters button functionality
    const clearButton = document.getElementById('clearFilters');
    if (clearButton) {
        clearButton.addEventListener('click', () => {
            // Clear all filter inputs
            document.querySelectorAll('.column-filter').forEach(filter => {
                filter.value = '';
            });
            
            // Clear date range displays
            document.querySelectorAll('.date-range').forEach(input => {
                input.value = '';
            });
            
            // Reset filters and refresh table
            columnFilters = {};
            currentPage = 1;
            populateCancelledJobsTable();
        });
    }
    
    // Add download CSV button functionality
    const downloadButton = document.getElementById('downloadCSV');
    if (downloadButton) {
        downloadButton.addEventListener('click', downloadFilteredCSV);
    }
    
    // Setup date range inputs
    setupDateRangeInputs();
    
    // Populate select dropdowns with unique values
    const selects = {
        'hiring_agency': [...new Set(jobData.cancelled_jobs.map(j => j.hiring_agency))].sort(),
        'location': [...new Set(jobData.cancelled_jobs.map(j => j.location || 'N/A'))].sort(),
        'grade': [...new Set(jobData.cancelled_jobs.map(j => {
            const grade = j.min_grade && j.max_grade ? 
                (j.min_grade === j.max_grade ? `GS-${j.min_grade}` : `GS-${j.min_grade}-${j.max_grade}`) : 
                j.min_grade ? `GS-${j.min_grade}` : 
                j.max_grade ? `GS-${j.max_grade}` : 'N/A';
            return grade;
        }))].sort(),
        'total_openings': [...new Set(jobData.cancelled_jobs.map(j => j.total_openings))].sort((a, b) => {
            // Put text values first (MANY, FEW)
            const aIsText = isNaN(parseInt(a));
            const bIsText = isNaN(parseInt(b));
            
            if (aIsText && !bIsText) return -1;  // Text before numbers
            if (!aIsText && bIsText) return 1;   // Numbers after text
            
            if (aIsText && bIsText) {
                // Both are text, alphabetical order
                return a.localeCompare(b);
            }
            
            // Both are numbers, sort numerically
            return parseInt(a) - parseInt(b);
        }),
        'service_type': [...new Set(jobData.cancelled_jobs.map(j => j.service_type || 'N/A'))].sort()
    };
    
    // Populate select dropdowns
    Object.keys(selects).forEach(column => {
        const select = document.querySelector(`.column-filter[data-column="${column}"]`);
        if (select) {
            selects[column].forEach(value => {
                if (value) {
                    const option = document.createElement('option');
                    option.value = value;
                    option.textContent = value;
                    select.appendChild(option);
                }
            });
        }
    });
    
    // Add event listeners to all filters
    document.querySelectorAll('.column-filter').forEach(filter => {
        filter.addEventListener('input', applyColumnFilters);
        filter.addEventListener('change', applyColumnFilters);
    });
}

function applyColumnFilters() {
    // Get all filter values
    columnFilters = {};
    const dateRanges = {};
    
    document.querySelectorAll('.column-filter').forEach(filter => {
        const column = filter.dataset.column;
        const value = filter.value.trim();
        
        // Skip the display inputs for date ranges
        if (filter.classList.contains('date-range')) {
            return;
        }
        
        // Handle date range filters differently
        if (filter.classList.contains('date-from') || filter.classList.contains('date-to')) {
            if (!dateRanges[column]) {
                dateRanges[column] = {};
            }
            if (filter.classList.contains('date-from') && value) {
                dateRanges[column].from = value;
            }
            if (filter.classList.contains('date-to') && value) {
                dateRanges[column].to = value;
            }
        } else if (value) {
            columnFilters[column] = value;
        }
    });
    
    // Add date ranges to columnFilters
    Object.keys(dateRanges).forEach(column => {
        if (dateRanges[column].from || dateRanges[column].to) {
            columnFilters[column] = dateRanges[column];
            console.log(`Date filter for ${column}:`, dateRanges[column]);
        }
    });
    
    // Apply filters
    let filtered = jobData.cancelled_jobs;
    
    Object.keys(columnFilters).forEach(column => {
        const filterValue = typeof columnFilters[column] === 'string' 
            ? columnFilters[column].toLowerCase() 
            : columnFilters[column]; // For date range objects
        
        filtered = filtered.filter(job => {
            let jobValue;
            
            // Handle special cases
            if (column === 'grade') {
                jobValue = job.min_grade && job.max_grade ? 
                    (job.min_grade === job.max_grade ? `GS-${job.min_grade}` : `GS-${job.min_grade}-${job.max_grade}`) : 
                    job.min_grade ? `GS-${job.min_grade}` : 
                    job.max_grade ? `GS-${job.max_grade}` : 'N/A';
            } else if (column === 'salary') {
                // Handle salary range filtering (e.g., ">50000")
                const minSalary = job.min_salary || 0;
                const maxSalary = job.max_salary || 0;
                if (filterValue.startsWith('>')) {
                    const threshold = parseInt(filterValue.substring(1));
                    return maxSalary > threshold;
                } else if (filterValue.startsWith('<')) {
                    const threshold = parseInt(filterValue.substring(1));
                    return minSalary < threshold;
                }
                jobValue = `${minSalary} ${maxSalary}`;
            } else if (column === 'days_open') {
                // Handle days open filtering (e.g., ">30")
                const days = job.days_open || 0;
                if (filterValue.startsWith('>')) {
                    const threshold = parseInt(filterValue.substring(1));
                    return days > threshold;
                } else if (filterValue.startsWith('<')) {
                    const threshold = parseInt(filterValue.substring(1));
                    return days < threshold;
                }
                jobValue = String(days);
            } else if (column === 'open_date' || column === 'close_date') {
                // Handle date range filtering
                const jobDate = job[column];
                if (!jobDate) return false;
                
                const jobDateObj = new Date(jobDate);
                const range = columnFilters[column];
                
                if (typeof range === 'object') {
                    // It's a range object
                    let inRange = true;
                    
                    if (range.from) {
                        const fromDate = new Date(range.from);
                        if (jobDateObj < fromDate) inRange = false;
                    }
                    
                    if (range.to) {
                        const toDate = new Date(range.to);
                        if (jobDateObj > toDate) inRange = false;
                    }
                    
                    return inRange;
                } else {
                    // Single date (shouldn't happen with new UI)
                    const filterDate = new Date(columnFilters[column]);
                    return jobDateObj.toDateString() === filterDate.toDateString();
                }
            } else {
                jobValue = job[column] || 'N/A';
            }
            
            // For select filters, do exact match
            if (document.querySelector(`.column-filter[data-column="${column}"]`).tagName === 'SELECT') {
                return jobValue === columnFilters[column];
            }
            
            // For text filters, do substring match
            return String(jobValue).toLowerCase().includes(filterValue.toLowerCase ? filterValue.toLowerCase() : filterValue);
        });
    });
    
    // Reset to page 1 when filtering
    currentPage = 1;
    populateCancelledJobsTable(filtered, currentPage);
    
    // Update clear button to show filter count
    const clearButton = document.getElementById('clearFilters');
    if (clearButton) {
        const filterCount = Object.keys(columnFilters).length;
        if (filterCount > 0) {
            clearButton.textContent = `Clear Filters (${filterCount})`;
            clearButton.style.backgroundColor = '#EDF2F7';
        } else {
            clearButton.textContent = 'Clear All Filters';
            clearButton.style.backgroundColor = 'white';
        }
    }
}


function updatePaginationControls(totalJobs, totalPages, filteredJobs) {
    // Update the header with count and pagination info
    const startJob = (currentPage - 1) * jobsPerPage + 1;
    const endJob = Math.min(currentPage * jobsPerPage, totalJobs);
    
    document.querySelector('.table-container h2').textContent = 
        `Cancelled Jobs Detail (Showing ${startJob}-${endJob} of ${totalJobs})`;
    
    // Create or update pagination controls
    let paginationDiv = document.getElementById('paginationControls');
    if (!paginationDiv) {
        paginationDiv = document.createElement('div');
        paginationDiv.id = 'paginationControls';
        paginationDiv.style.cssText = 'text-align: center; margin: 20px 0; display: flex; justify-content: center; align-items: center; gap: 10px;';
        
        const tableContainer = document.querySelector('.table-container');
        tableContainer.appendChild(paginationDiv);
    }
    
    // Clear existing controls
    paginationDiv.innerHTML = '';
    
    // Previous button
    const prevButton = document.createElement('button');
    prevButton.textContent = '← Previous';
    prevButton.disabled = currentPage === 1;
    prevButton.style.cssText = 'padding: 8px 16px; border: 1px solid #e2e8f0; background: white; cursor: pointer; border-radius: 6px; font-weight: 500; color: #4a5568; transition: all 0.15s;';
    if (!prevButton.disabled) {
        prevButton.onclick = () => {
            currentPage--;
            populateCancelledJobsTable(filteredJobs, currentPage);
        };
    }
    paginationDiv.appendChild(prevButton);
    
    // Page info
    const pageInfo = document.createElement('span');
    pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
    pageInfo.style.cssText = 'padding: 0 20px; font-weight: bold;';
    paginationDiv.appendChild(pageInfo);
    
    // Next button
    const nextButton = document.createElement('button');
    nextButton.textContent = 'Next →';
    nextButton.disabled = currentPage === totalPages;
    nextButton.style.cssText = 'padding: 8px 16px; border: 1px solid #e2e8f0; background: white; cursor: pointer; border-radius: 6px; font-weight: 500; color: #4a5568; transition: all 0.15s;';
    if (!nextButton.disabled) {
        nextButton.onclick = () => {
            currentPage++;
            populateCancelledJobsTable(filteredJobs, currentPage);
        };
    }
    paginationDiv.appendChild(nextButton);
    
    // Page size selector
    const pageSizeLabel = document.createElement('span');
    pageSizeLabel.textContent = ' | Show: ';
    pageSizeLabel.style.cssText = 'margin-left: 20px;';
    paginationDiv.appendChild(pageSizeLabel);
    
    const pageSizeSelect = document.createElement('select');
    pageSizeSelect.style.cssText = 'padding: 4px 8px; border: 1px solid #e2e8f0; border-radius: 6px; font-weight: 500; color: #4a5568;';
    [50, 100, 200, 500].forEach(size => {
        const option = document.createElement('option');
        option.value = size;
        option.textContent = `${size} per page`;
        option.selected = size === jobsPerPage;
        pageSizeSelect.appendChild(option);
    });
    pageSizeSelect.onchange = (e) => {
        jobsPerPage = parseInt(e.target.value);
        currentPage = 1;
        populateCancelledJobsTable(filteredJobs, currentPage);
    };
    paginationDiv.appendChild(pageSizeSelect);
}


function setupDateRangeInputs() {
    document.querySelectorAll('.date-range').forEach(rangeInput => {
        const column = rangeInput.dataset.column;
        const fromInput = document.querySelector(`.date-from[data-column="${column}"]`);
        const toInput = document.querySelector(`.date-to[data-column="${column}"]`);
        
        // Create a simple date picker popup
        rangeInput.addEventListener('click', (e) => {
            e.stopPropagation();
            
            // Create popup if it doesn't exist
            let popup = rangeInput.nextElementSibling;
            if (!popup || !popup.classList.contains('date-picker-popup')) {
                popup = document.createElement('div');
                popup.className = 'date-picker-popup';
                popup.innerHTML = `
                    <div style="margin-bottom: 8px;">
                        <label style="font-size: 12px; color: #4a5568; display: block;">From:</label>
                        <input type="date" class="popup-from">
                    </div>
                    <div style="margin-bottom: 12px;">
                        <label style="font-size: 12px; color: #4a5568; display: block;">To:</label>
                        <input type="date" class="popup-to">
                    </div>
                    <button class="apply-date-range" style="padding: 4px 12px; background: #4299E1; color: white; border: none; border-radius: 4px; font-size: 12px; cursor: pointer; margin-right: 8px;">Apply</button>
                    <button class="clear-date-range" style="padding: 4px 12px; background: #E2E8F0; color: #4a5568; border: none; border-radius: 4px; font-size: 12px; cursor: pointer;">Clear</button>
                `;
                
                // Prevent clicks inside popup from closing it
                popup.addEventListener('click', (e) => {
                    e.stopPropagation();
                });
                rangeInput.parentElement.appendChild(popup);
                
                // Position the popup
                popup.style.top = (rangeInput.offsetHeight + 2) + 'px';
                popup.style.left = '0';
                
                // Set initial values
                const popupFrom = popup.querySelector('.popup-from');
                const popupTo = popup.querySelector('.popup-to');
                popupFrom.value = fromInput.value;
                popupTo.value = toInput.value;
                
                // Apply button
                popup.querySelector('.apply-date-range').addEventListener('click', () => {
                    fromInput.value = popupFrom.value;
                    toInput.value = popupTo.value;
                    
                    console.log(`Setting date range for ${column}: from=${popupFrom.value}, to=${popupTo.value}`);
                    
                    // Update display
                    updateDateRangeDisplay(rangeInput, popupFrom.value, popupTo.value);
                    
                    // Hide popup
                    popup.style.display = 'none';
                    
                    // Apply filters
                    applyColumnFilters();
                });
                
                // Clear button
                popup.querySelector('.clear-date-range').addEventListener('click', () => {
                    fromInput.value = '';
                    toInput.value = '';
                    rangeInput.value = '';
                    popup.style.display = 'none';
                    applyColumnFilters();
                });
            }
            
            // Toggle popup
            popup.style.display = popup.style.display === 'block' ? 'none' : 'block';
        });
    });
    
    // Close popups when clicking outside
    document.addEventListener('click', () => {
        document.querySelectorAll('.date-picker-popup').forEach(popup => {
            popup.style.display = 'none';
        });
    });
}

function updateDateRangeDisplay(input, fromDate, toDate) {
    if (fromDate && toDate) {
        const from = new Date(fromDate).toLocaleDateString();
        const to = new Date(toDate).toLocaleDateString();
        input.value = `${from} - ${to}`;
    } else if (fromDate) {
        const from = new Date(fromDate).toLocaleDateString();
        input.value = `From ${from}`;
    } else if (toDate) {
        const to = new Date(toDate).toLocaleDateString();
        input.value = `To ${to}`;
    } else {
        input.value = '';
    }
}

function downloadFilteredCSV() {
    // Get the currently filtered data or all cancelled jobs
    let dataToExport = jobData.cancelled_jobs;
    
    // Apply current filters if any
    if (Object.keys(columnFilters).length > 0) {
        // Re-apply filters to get filtered data
        dataToExport = jobData.cancelled_jobs.filter(job => {
            return Object.keys(columnFilters).every(column => {
                const filterValue = typeof columnFilters[column] === 'string' 
                    ? columnFilters[column].toLowerCase() 
                    : columnFilters[column];
                
                let jobValue;
                
                // Apply same filtering logic as in applyColumnFilters
                if (column === 'grade') {
                    jobValue = job.min_grade && job.max_grade ? 
                        (job.min_grade === job.max_grade ? `GS-${job.min_grade}` : `GS-${job.min_grade}-${job.max_grade}`) : 
                        job.min_grade ? `GS-${job.min_grade}` : 
                        job.max_grade ? `GS-${job.max_grade}` : 'N/A';
                } else if (column === 'salary') {
                    const minSalary = job.min_salary || 0;
                    const maxSalary = job.max_salary || 0;
                    if (typeof filterValue === 'string') {
                        if (filterValue.startsWith('>')) {
                            const threshold = parseInt(filterValue.substring(1));
                            return maxSalary > threshold;
                        } else if (filterValue.startsWith('<')) {
                            const threshold = parseInt(filterValue.substring(1));
                            return minSalary < threshold;
                        }
                    }
                    jobValue = `${minSalary} ${maxSalary}`;
                } else if (column === 'days_open') {
                    const days = job.days_open || 0;
                    if (typeof filterValue === 'string') {
                        if (filterValue.startsWith('>')) {
                            const threshold = parseInt(filterValue.substring(1));
                            return days > threshold;
                        } else if (filterValue.startsWith('<')) {
                            const threshold = parseInt(filterValue.substring(1));
                            return days < threshold;
                        }
                    }
                    jobValue = String(days);
                } else if (column === 'open_date' || column === 'close_date') {
                    const jobDate = job[column];
                    if (!jobDate) return false;
                    
                    const jobDateObj = new Date(jobDate);
                    const range = columnFilters[column];
                    
                    if (typeof range === 'object') {
                        let inRange = true;
                        if (range.from) {
                            const fromDate = new Date(range.from);
                            if (jobDateObj < fromDate) inRange = false;
                        }
                        if (range.to) {
                            const toDate = new Date(range.to);
                            if (jobDateObj > toDate) inRange = false;
                        }
                        return inRange;
                    }
                    return true;
                } else {
                    jobValue = job[column] || 'N/A';
                }
                
                // Check filter match
                if (document.querySelector(`.column-filter[data-column="${column}"]`) && 
                    document.querySelector(`.column-filter[data-column="${column}"]`).tagName === 'SELECT') {
                    return jobValue === columnFilters[column];
                }
                
                return String(jobValue).toLowerCase().includes(filterValue.toLowerCase ? filterValue.toLowerCase() : filterValue);
            });
        });
    }
    
    // Create CSV content
    const headers = [
        'Control Number',
        'Position Title',
        'Hiring Agency',
        'Location',
        'Open Date',
        'Close Date',
        'Days Open',
        'Grade',
        'Min Salary',
        'Max Salary',
        'Total Openings',
        'Service Type',
        'Work Schedule',
        'Supervisory Status',
        'Drug Test Required',
        'Security Clearance',
        'USAJobs URL'
    ];
    
    let csvContent = headers.join(',') + '\n';
    
    dataToExport.forEach(job => {
        const grade = job.min_grade && job.max_grade ? 
            (job.min_grade === job.max_grade ? `GS-${job.min_grade}` : `GS-${job.min_grade}-${job.max_grade}`) : 
            job.min_grade ? `GS-${job.min_grade}` : 
            job.max_grade ? `GS-${job.max_grade}` : 'N/A';
        
        const row = [
            job.control_number,
            `"${job.position_title.replace(/"/g, '""')}"`,
            `"${(job.hiring_agency || 'N/A').replace(/"/g, '""')}"`,
            `"${(job.location || 'N/A').replace(/"/g, '""')}"`,
            job.open_date,
            job.close_date,
            job.days_open !== null ? job.days_open : 'N/A',
            grade,
            job.min_salary || 0,
            job.max_salary || 0,
            job.total_openings || 'N/A',
            job.service_type || 'N/A',
            job.work_schedule || 'N/A',
            job.supervisory_status || 'N/A',
            job.drug_test_required || 'N/A',
            job.security_clearance || 'N/A',
            job.usajobs_url
        ];
        
        csvContent += row.join(',') + '\n';
    });
    
    // Create download
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    
    // Generate filename with date
    const today = new Date().toISOString().split('T')[0];
    const filterStatus = Object.keys(columnFilters).length > 0 ? '_filtered' : '';
    const filename = `cancelled_jobs_${today}${filterStatus}.csv`;
    
    link.setAttribute('href', url);
    link.setAttribute('download', filename);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    // Update button to show count
    const downloadButton = document.getElementById('downloadCSV');
    const originalText = downloadButton.textContent;
    downloadButton.textContent = `Downloaded ${dataToExport.length} jobs`;
    setTimeout(() => {
        downloadButton.textContent = originalText;
    }, 2000);
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    loadData();
});