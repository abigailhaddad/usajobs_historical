// Federal Job Listings Tracker - Implementation matching budget app structure

let dataTable;
let jobListingsData = [];
let columnFilters = {};
let departmentData = [];
let agencyData = [];
let aggregationLevel = 'department'; // 'individual', 'agency', or 'department'

// Define department colors based on actual departments in our data
const DEPARTMENT_COLORS = {
    // Major Cabinet Departments
    "Department of Agriculture": "#2ca02c",
    "Department of Commerce": "#d62728",
    "Department of Defense": "#9467bd",
    "Department of Education": "#8c564b",
    "Department of Energy": "#e377c2",
    "Department of Health and Human Services": "#7f7f7f",
    "Department of Homeland Security": "#bcbd22",
    "Department of Housing and Urban Development": "#17becf",
    "Department of the Interior": "#aec7e8",
    "Department of Justice": "#ffbb78",
    "Department of Labor": "#98df8a",
    "Department of State": "#ff9896",
    "Department of Transportation": "#c5b0d5",
    "Department of the Treasury": "#c49c94",
    "Department of Veterans Affairs": "#f7b6d2",
    
    // Defense Components
    "Department of the Air Force": "#c7c7c7",
    "Department of the Army": "#dbdb8d",
    "Department of the Navy": "#9edae5",
    
    // Branches of Government
    "Legislative Branch": "#1f77b4",
    "Judicial Branch": "#ff7f0e",
    "Executive Office of the President": "#393b79",
    
    // Independent Agencies
    "General Services Administration": "#5254a3",
    "National Aeronautics and Space Administration": "#9c9ede",
    "National Foundation on the Arts and the Humanities": "#e7ba52",
    "Court Services and Offender Supervision Agency for DC": "#ad494a",
    
    // Catch-all categories
    "Other Agencies and Independent Organizations": "#e7cb94",
    "Unknown": "#999999"
};

// Format number with commas
function formatNumber(value) {
    return value.toLocaleString();
}

// Format percentage
function formatPercentage(value) {
    return `${value.toFixed(1)}%`;
}

// Load CSV data
async function loadData() {
    try {
        console.log('Starting to load CSV data...');
        const response = await fetch('data/job_listings_summary.csv');
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const csvText = await response.text();
        console.log('CSV loaded successfully, length:', csvText.length);
        
        // Parse CSV
        const lines = csvText.split('\n');
        const headers = lines[0].split(',');
        
        jobListingsData = [];
        for (let i = 1; i < lines.length; i++) {
            if (lines[i].trim() === '') continue;
            
            // Handle CSV parsing with proper quote handling
            const values = parseCSVLine(lines[i]);
            if (values.length !== headers.length) continue;
            
            const row = {};
            headers.forEach((header, index) => {
                row[header.trim()] = values[index];
            });
            
            // Convert numeric values
            row.listings2024Value = parseInt(row['Listings_2024'].replace(/,/g, '')) || 0;
            row.listings2025Value = parseInt(row['Listings_2025'].replace(/,/g, '')) || 0;
            row.percentageValue = parseFloat(row['Percentage_2025_of_2024'].replace('%', '')) || 0;
            
            jobListingsData.push(row);
        }
        
        // Calculate department and agency-level aggregations
        aggregateDepartmentData();
        aggregateAgencyData();
        
        // Calculate summary statistics
        updateSummaryStats();
        
        // Populate main filters
        populateMainFilters();
        
        // Initialize DataTable
        initializeDataTable();
        
        // Initialize bubble chart
        initializeBubbleChart();
        
    } catch (error) {
        console.error('Error loading data:', error);
        alert('Error loading data. Please ensure the server is running.');
    }
}

// Parse CSV line handling quoted values
function parseCSVLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
        const char = line[i];
        
        if (char === '"') {
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            result.push(current.trim().replace(/^"|"$/g, ''));
            current = '';
        } else {
            current += char;
        }
    }
    
    if (current) {
        result.push(current.trim().replace(/^"|"$/g, ''));
    }
    
    return result;
}

// Aggregate data by department
function aggregateDepartmentData() {
    const deptMap = new Map();
    
    jobListingsData.forEach(row => {
        const dept = row.Department || 'Unknown';
        
        if (!deptMap.has(dept)) {
            deptMap.set(dept, {
                name: dept,
                listings2024: 0,
                listings2025: 0,
                agencyCount: new Set()
            });
        }
        
        const deptInfo = deptMap.get(dept);
        deptInfo.listings2024 += row.listings2024Value;
        deptInfo.listings2025 += row.listings2025Value;
        if (row.Agency) {
            deptInfo.agencyCount.add(row.Agency);
        }
    });
    
    // Convert to array and calculate percentages
    departmentData = Array.from(deptMap.values()).map(dept => ({
        ...dept,
        agencyCount: dept.agencyCount.size,
        percentageOf2024: dept.listings2024 > 0 ? 
            (dept.listings2025 / dept.listings2024 * 100) : 0
    }));
    
    // Sort by 2024 listings descending
    departmentData.sort((a, b) => b.listings2024 - a.listings2024);
}

// Aggregate data by agency
function aggregateAgencyData() {
    const agencyMap = new Map();
    
    jobListingsData.forEach(row => {
        const agency = row.Agency || 'Unknown';
        const dept = row.Department || 'Unknown';
        const key = `${dept}|${agency}`;
        
        if (!agencyMap.has(key)) {
            agencyMap.set(key, {
                department: dept,
                name: agency,
                listings2024: 0,
                listings2025: 0,
                recordCount: 0
            });
        }
        
        const agencyInfo = agencyMap.get(key);
        agencyInfo.listings2024 += row.listings2024Value;
        agencyInfo.listings2025 += row.listings2025Value;
        agencyInfo.recordCount += 1;
    });
    
    // Convert to array and calculate percentages
    agencyData = Array.from(agencyMap.values()).map(agency => ({
        ...agency,
        percentageOf2024: agency.listings2024 > 0 ? 
            (agency.listings2025 / agency.listings2024 * 100) : 0
    }));
    
    // Sort by 2024 listings descending
    agencyData.sort((a, b) => b.listings2024 - a.listings2024);
}

// Update summary statistics
function updateSummaryStats() {
    const total2024 = jobListingsData.reduce((sum, row) => sum + row.listings2024Value, 0);
    const total2025 = jobListingsData.reduce((sum, row) => sum + row.listings2025Value, 0);
    const overallPercentage = total2024 > 0 ? (total2025 / total2024 * 100) : 0;
    
    $('#total2024').text(formatNumber(total2024));
    $('#total2025').text(formatNumber(total2025));
    $('#overallPercentage').text(formatPercentage(overallPercentage));
    $('#entityCount').text(departmentData.length);
}

// Populate main filter dropdowns
function populateMainFilters() {
    // Get unique values
    const departments = [...new Set(jobListingsData.map(row => row.Department))].sort();
    const agencies = [...new Set(jobListingsData.map(row => row.Agency))].sort();
    const appointmentTypes = [...new Set(jobListingsData.map(row => row.Appointment_Type))].sort();
    const workSchedules = [...new Set(jobListingsData.map(row => row.Work_Schedule))].sort();
    const hiringPaths = [...new Set(jobListingsData.map(row => row.Hiring_Paths))].sort();
    
    // Populate dropdowns
    populateDropdown('mainDepartmentFilter', departments);
    populateDropdown('mainAgencyFilter', agencies);
    populateDropdown('mainAppointmentFilter', appointmentTypes);
    populateDropdown('mainScheduleFilter', workSchedules);
    populateDropdown('mainHiringPathFilter', hiringPaths);
}

// Populate a dropdown
function populateDropdown(selectId, options) {
    const $select = $(`#${selectId}`);
    const currentValue = $select.val();
    
    $select.empty();
    $select.append('<option value="">All</option>');
    
    options.forEach(option => {
        $select.append(`<option value="${option}">${option}</option>`);
    });
    
    // Restore previous selection if it exists
    if (currentValue && options.includes(currentValue)) {
        $select.val(currentValue);
    }
}

// Initialize DataTable
function initializeDataTable() {
    // Initialize with empty data - will be populated by updateTableData
    dataTable = $('#jobsTable').DataTable({
        data: [],
        columns: [
            { 
                data: 'Department',
                render: function(data) {
                    const color = DEPARTMENT_COLORS[data] || '#999999';
                    return `<span style="display: inline-block; width: 10px; height: 10px; background-color: ${color}; border-radius: 50%; margin-right: 8px;"></span>${data}`;
                }
            },
            { data: 'Agency' },
            { 
                data: 'listings2024',
                render: function(data) {
                    return formatNumber(data);
                }
            },
            { 
                data: 'listings2025',
                render: function(data) {
                    return formatNumber(data);
                }
            },
            { 
                data: null,
                render: function(data) {
                    const change = data.listings2025 - data.listings2024;
                    const sign = change >= 0 ? '+' : '';
                    // Use red for decreases, green for increases
                    const className = change >= 0 ? 'high-percentage' : 'low-percentage';
                    return `<span class="${className}">${sign}${formatNumber(change)}</span>`;
                }
            },
            { 
                data: 'percentageOf2024',
                render: function(data) {
                    // Color based on how close to 100% (maintaining 2024 levels)
                    const className = data >= 100 ? 'high-percentage' :   // Green if maintained/grew
                                    data >= 75 ? 'medium-percentage' :    // Yellow if moderate decline
                                    'low-percentage';                      // Red if major decline
                    return `<span class="percentage-cell ${className}">${formatPercentage(data)}</span>`;
                }
            }
        ],
        order: [[2, 'desc']], // Sort by 2024 listings
        pageLength: 25,
        lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "All"]],
        responsive: true,
        dom: 'lfrtip'
    });
    
    // Show appropriate data based on aggregation level
    updateTableData();
}

// Update table data based on current filters and aggregation level
function updateTableData() {
    if (!dataTable) return;
    
    let dataToShow;
    if (aggregationLevel === 'department') {
        dataToShow = getFilteredDepartmentData().map(d => ({
            Department: d.name,
            Agency: '—',
            listings2024: d.listings2024,
            listings2025: d.listings2025,
            percentageOf2024: d.percentageOf2024
        }));
    } else if (aggregationLevel === 'agency') {
        dataToShow = getFilteredAgencyData().map(d => ({
            Department: d.department,
            Agency: d.name,
            listings2024: d.listings2024,
            listings2025: d.listings2025,
            percentageOf2024: d.percentageOf2024
        }));
    } else {
        // Individual records with filters applied
        dataToShow = jobListingsData.filter(row => {
            const deptFilter = $('#mainDepartmentFilter').val();
            if (deptFilter && row.Department !== deptFilter) return false;
            
            const agencyFilter = $('#mainAgencyFilter').val();
            if (agencyFilter && row.Agency !== agencyFilter) return false;
            
            const apptFilter = $('#mainAppointmentFilter').val();
            if (apptFilter && row.Appointment_Type !== apptFilter) return false;
            
            const scheduleFilter = $('#mainScheduleFilter').val();
            if (scheduleFilter && row.Work_Schedule !== scheduleFilter) return false;
            
            const pathFilter = $('#mainHiringPathFilter').val();
            if (pathFilter && row.Hiring_Paths !== pathFilter) return false;
            
            return true;
        }).map(row => ({
            Department: row.Department,
            Agency: row.Agency,
            listings2024: row.listings2024Value,
            listings2025: row.listings2025Value,
            percentageOf2024: row.percentageValue
        }));
    }
    
    // Clear and add new data
    dataTable.clear();
    dataTable.rows.add(dataToShow);
    dataTable.draw();
}


// Initialize bubble chart
function initializeBubbleChart() {
    const container = d3.select('#bubble-chart');
    container.selectAll('*').remove();
    
    // Get dimensions
    const margin = {top: 20, right: 40, bottom: 60, left: 40};
    const width = container.node().getBoundingClientRect().width - margin.left - margin.right;
    const height = 400 - margin.top - margin.bottom;
    
    // Get data based on aggregation level
    let dataToShow;
    if (aggregationLevel === 'department') {
        dataToShow = getFilteredDepartmentData();
    } else {
        dataToShow = getFilteredAgencyData();
    }
    
    // Filter out entities with no 2024 listings
    dataToShow = dataToShow.filter(d => d.listings2024 > 0);
    
    // Create scales - dynamically based on actual data
    const xMax = d3.max(dataToShow, d => d.percentageOf2024) || 100;
    const xScale = d3.scaleLinear()
        .domain([0, Math.max(100, xMax * 1.1)]) // Add 10% padding to max value
        .range([0, width]);
    
    // Size scale for bubbles
    const maxListings = d3.max(dataToShow, d => d.listings2024);
    const sizeScale = d3.scaleSqrt()
        .domain([0, maxListings])
        .range([8, 60]);
    
    // Create force simulation
    const simulation = d3.forceSimulation(dataToShow)
        .force('x', d3.forceX(d => xScale(d.percentageOf2024)).strength(1))
        .force('y', d3.forceY(height / 2).strength(0.1))
        .force('collide', d3.forceCollide(d => sizeScale(d.listings2024) + 2))
        .stop();
    
    // Run simulation
    for (let i = 0; i < 120; ++i) simulation.tick();
    
    // Create SVG
    const svg = container
        .append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom);
    
    const g = svg.append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);
    
    // Add grid
    g.append('g')
        .attr('class', 'grid')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(xScale)
            .tickSize(-height)
            .tickFormat(''));
    
    // Add x-axis
    g.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(xScale).tickFormat(d => d + '%'));
    
    // Add axis label
    g.append('text')
        .attr('class', 'axis-label')
        .attr('x', width / 2)
        .attr('y', height + 45)
        .style('text-anchor', 'middle')
        .text('2025 Listings as % of 2024 Listings');
    
    // Add 100% line
    if (xScale.domain()[1] >= 100) {
        g.append('line')
            .attr('x1', xScale(100))
            .attr('x2', xScale(100))
            .attr('y1', 0)
            .attr('y2', height)
            .style('stroke', '#666')
            .style('stroke-dasharray', '3,3');
        
        g.append('text')
            .attr('x', xScale(100) + 5)
            .attr('y', 20)
            .style('font-size', '11px')
            .style('fill', '#666')
            .text('Same as 2024');
    }
    
    // Create tooltip
    const tooltip = d3.select('body').append('div')
        .attr('class', 'tooltip')
        .style('opacity', 0)
        .style('position', 'absolute')
        .style('background', 'rgba(0, 0, 0, 0.8)')
        .style('color', 'white')
        .style('padding', '10px')
        .style('border-radius', '4px')
        .style('font-size', '12px')
        .style('pointer-events', 'none');
    
    // Add bubbles
    const bubbles = g.selectAll('.bubble')
        .data(dataToShow)
        .enter().append('circle')
        .attr('class', 'bubble')
        .attr('cx', d => d.x)
        .attr('cy', d => d.y)
        .attr('r', d => sizeScale(d.listings2024))
        .attr('fill', d => {
            const dept = aggregationLevel === 'department' ? d.name : d.department;
            return DEPARTMENT_COLORS[dept] || '#999999';
        })
        .style('opacity', 0.8)
        .style('stroke', '#333')
        .style('stroke-width', 1)
        .on('mouseover', function(event, d) {
            tooltip.transition()
                .duration(200)
                .style('opacity', .9);
            
            let content;
            if (aggregationLevel === 'department') {
                content = `
                    <strong>${d.name}</strong><br/>
                    2024 Listings: ${formatNumber(d.listings2024)}<br/>
                    2025 Listings: ${formatNumber(d.listings2025)}<br/>
                    2025 as % of 2024: ${formatPercentage(d.percentageOf2024)}<br/>
                    Agencies: ${d.agencyCount}
                `;
            } else {
                content = `
                    <strong>${d.name}</strong><br/>
                    Department: ${d.department}<br/>
                    2024 Listings: ${formatNumber(d.listings2024)}<br/>
                    2025 Listings: ${formatNumber(d.listings2025)}<br/>
                    2025 as % of 2024: ${formatPercentage(d.percentageOf2024)}
                `;
            }
            
            tooltip.html(content)
                .style('left', (event.pageX + 10) + 'px')
                .style('top', (event.pageY - 28) + 'px');
        })
        .on('mouseout', function() {
            tooltip.transition()
                .duration(500)
                .style('opacity', 0);
        });
    
    // Update entity count
    $('#entityCount').text(dataToShow.length);
}

// Get filtered department data
function getFilteredDepartmentData() {
    // Apply filters to raw data first
    const filteredRaw = jobListingsData.filter(row => {
        const deptFilter = $('#mainDepartmentFilter').val();
        if (deptFilter && row.Department !== deptFilter) return false;
        
        const agencyFilter = $('#mainAgencyFilter').val();
        if (agencyFilter && row.Agency !== agencyFilter) return false;
        
        const apptFilter = $('#mainAppointmentFilter').val();
        if (apptFilter && row.Appointment_Type !== apptFilter) return false;
        
        const scheduleFilter = $('#mainScheduleFilter').val();
        if (scheduleFilter && row.Work_Schedule !== scheduleFilter) return false;
        
        const pathFilter = $('#mainHiringPathFilter').val();
        if (pathFilter && row.Hiring_Paths !== pathFilter) return false;
        
        return true;
    });
    
    // Re-aggregate by department
    const deptMap = new Map();
    filteredRaw.forEach(row => {
        const dept = row.Department || 'Unknown';
        
        if (!deptMap.has(dept)) {
            deptMap.set(dept, {
                name: dept,
                listings2024: 0,
                listings2025: 0,
                agencyCount: new Set()
            });
        }
        
        const deptInfo = deptMap.get(dept);
        deptInfo.listings2024 += row.listings2024Value;
        deptInfo.listings2025 += row.listings2025Value;
        if (row.Agency) {
            deptInfo.agencyCount.add(row.Agency);
        }
    });
    
    return Array.from(deptMap.values()).map(dept => ({
        ...dept,
        agencyCount: dept.agencyCount.size,
        percentageOf2024: dept.listings2024 > 0 ? 
            (dept.listings2025 / dept.listings2024 * 100) : 0
    }));
}

// Get filtered agency data
function getFilteredAgencyData() {
    // Apply filters to raw data first
    const filteredRaw = jobListingsData.filter(row => {
        const deptFilter = $('#mainDepartmentFilter').val();
        if (deptFilter && row.Department !== deptFilter) return false;
        
        const agencyFilter = $('#mainAgencyFilter').val();
        if (agencyFilter && row.Agency !== agencyFilter) return false;
        
        const apptFilter = $('#mainAppointmentFilter').val();
        if (apptFilter && row.Appointment_Type !== apptFilter) return false;
        
        const scheduleFilter = $('#mainScheduleFilter').val();
        if (scheduleFilter && row.Work_Schedule !== scheduleFilter) return false;
        
        const pathFilter = $('#mainHiringPathFilter').val();
        if (pathFilter && row.Hiring_Paths !== pathFilter) return false;
        
        return true;
    });
    
    // Re-aggregate by agency
    const agencyMap = new Map();
    filteredRaw.forEach(row => {
        const agency = row.Agency || 'Unknown';
        const dept = row.Department || 'Unknown';
        const key = `${dept}|${agency}`;
        
        if (!agencyMap.has(key)) {
            agencyMap.set(key, {
                department: dept,
                name: agency,
                listings2024: 0,
                listings2025: 0
            });
        }
        
        const agencyInfo = agencyMap.get(key);
        agencyInfo.listings2024 += row.listings2024Value;
        agencyInfo.listings2025 += row.listings2025Value;
    });
    
    return Array.from(agencyMap.values()).map(agency => ({
        ...agency,
        percentageOf2024: agency.listings2024 > 0 ? 
            (agency.listings2025 / agency.listings2024 * 100) : 0
    }));
}

// Initialize event handlers
function initializeEventHandlers() {
    // Filter changes
    $('#mainDepartmentFilter, #mainAgencyFilter, #mainAppointmentFilter, #mainScheduleFilter, #mainHiringPathFilter').on('change', function() {
        updateTableData();
        updateFilteredStats();
        initializeBubbleChart();
    });
    
    // Aggregation level change
    $('#aggregationLevel').on('change', function() {
        aggregationLevel = $(this).val();
        updateTableData();
        initializeBubbleChart();
        
        // Update entity label
        const label = aggregationLevel === 'department' ? 'Departments' : 'Agencies';
        $('#entityCount').siblings('.stat-label').text(label);
    });
}

// Update filtered statistics
function updateFilteredStats() {
    // Get the current table data (already filtered)
    const tableData = dataTable.data();
    
    let total2024 = 0;
    let total2025 = 0;
    let count = 0;
    
    tableData.each(function(row) {
        total2024 += row.listings2024;
        total2025 += row.listings2025;
        count++;
    });
    
    const percentage = total2024 > 0 ? (total2025 / total2024 * 100) : 0;
    
    $('#total2024').text(formatNumber(total2024));
    $('#total2025').text(formatNumber(total2025));
    $('#overallPercentage').text(formatPercentage(percentage));
    $('#entityCount').text(count);
}

// Initialize on document ready
$(document).ready(function() {
    loadData();
    initializeEventHandlers();
});