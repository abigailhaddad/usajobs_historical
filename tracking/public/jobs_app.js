// Federal Job Listings Tracker - Implementation matching budget app structure

let dataTable;
let jobListingsData = [];
let columnFilters = {};
let departmentData = [];
let agencyData = [];
let subagencyData = [];
let departmentMetadata = {};
let aggregationLevel = 'department'; // 'individual', 'agency', 'department', or 'subagency'
let occupationSeriesMap = {};
let currentDepartmentData = null; // Store department-specific data when loaded

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

// Load occupation series mapping
async function loadOccupationMap() {
    try {
        // Try to load the data-generated map first
        let response = await fetch('occupation_series_from_data.json');
        if (response.ok) {
            occupationSeriesMap = await response.json();
            console.log('Loaded occupation map from data with', Object.keys(occupationSeriesMap).length, 'entries');
            return;
        }
        
        // Fall back to the original map if the new one isn't available
        response = await fetch('occupation_series_complete.json');
        if (response.ok) {
            occupationSeriesMap = await response.json();
            console.log('Loaded fallback occupation map with', Object.keys(occupationSeriesMap).length, 'entries');
        } else {
            console.log('Failed to load occupation map, status:', response.status);
        }
    } catch (error) {
        console.log('Could not load occupation series map, using codes only:', error);
    }
}

// Update date display from stats
function updateDateDisplay(stats) {
    if (stats && stats.date_range) {
        const prev = stats.date_range.previous_year;
        const curr = stats.date_range.current_year;
        
        // Update subtitle
        document.getElementById('dateRangeSubtitle').textContent = 
            `Comparing ${prev.display} vs ${curr.display}`;
        
        // Update stat labels with full date ranges
        document.getElementById('previousYearLabel').innerHTML = `Listings<br>${prev.display}`;
        document.getElementById('currentYearLabel').innerHTML = `Listings<br>${curr.display}`;
        document.getElementById('percentageLabel').textContent = `${curr.year} as % of ${prev.year}`;
        
        // Update last updated date
        if (stats.generated_at) {
            const genDate = new Date(stats.generated_at);
            document.getElementById('lastUpdated').textContent = 
                `Last updated: ${genDate.toLocaleDateString()}`;
        }
    }
}

// Load department-specific data with subagency information
async function loadDepartmentData(departmentName) {
    try {
        // Find the department metadata
        const deptMeta = departmentMetadata.find(d => d.department === departmentName);
        if (!deptMeta) {
            console.error('Department metadata not found for:', departmentName);
            return null;
        }
        
        console.log(`Loading department data for ${departmentName} from ${deptMeta.filename}`);
        const response = await fetch(`data/departments/${deptMeta.filename}`);
        
        if (!response.ok) {
            throw new Error(`Failed to load department data: ${response.status}`);
        }
        
        const deptData = await response.json();
        console.log(`Loaded ${deptData.length} rows for ${departmentName}`);
        return deptData;
    } catch (error) {
        console.error('Error loading department data:', error);
        return null;
    }
}

// Load CSV data
async function loadData() {
    try {
        // Load occupation map first
        await loadOccupationMap();
        
        // Load department metadata
        const metaResponse = await fetch('data/department_metadata.json');
        if (metaResponse.ok) {
            departmentMetadata = await metaResponse.json();
            console.log('Loaded department metadata for', departmentMetadata.length, 'departments');
        }
        
        // Load stats file first to get date ranges
        const statsResponse = await fetch('data/job_listings_stats.json');
        if (statsResponse.ok) {
            const stats = await statsResponse.json();
            updateDateDisplay(stats);
        }
        
        console.log('Starting to load JSON data...');
        const response = await fetch('data/job_listings_summary.json');
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        jobListingsData = await response.json();
        console.log('JSON loaded successfully, rows:', jobListingsData.length);
        
        // Debug: Check sample data
        if (jobListingsData.length > 0) {
            console.log('Sample row:', jobListingsData[0]);
            console.log('Occupation series in first 10 rows:', 
                jobListingsData.slice(0, 10).map(r => r.Occupation_Series));
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
    // Get unique departments
    const departments = [...new Set(jobListingsData.map(row => row.Department))].sort();
    
    // Populate department dropdown
    populateDropdown('mainDepartmentFilter', departments);
    
    // Update dependent filters based on department selection
    updateDependentFilters();
    
    // Change handlers will be set up in initializeEventHandlers()
}

// Update dependent filter dropdowns based on current selections
function updateDependentFilters() {
    const selectedDepartment = $('#mainDepartmentFilter').val();
    const selectedAgency = $('#mainAgencyFilter').val();
    const selectedAppointment = $('#mainAppointmentFilter').val();
    
    // Filter data based on current selections for cascading
    let filteredData = jobListingsData;
    
    // Apply department filter
    if (selectedDepartment) {
        filteredData = filteredData.filter(row => row.Department === selectedDepartment);
    }
    
    // Apply agency filter for subsequent dropdowns
    if (selectedAgency) {
        filteredData = filteredData.filter(row => row.Agency === selectedAgency);
    }
    
    // Apply appointment type filter for occupation dropdown
    if (selectedAppointment) {
        filteredData = filteredData.filter(row => row.Appointment_Type === selectedAppointment);
    }
    
    // Get current selections to preserve them if possible
    const currentAgency = $('#mainAgencyFilter').val();
    const currentAppointment = $('#mainAppointmentFilter').val();
    const currentOccupation = $('#mainOccupationFilter').val();
    
    // Rebuild agency dropdown (filtered by department only)
    let agencyFilterData = jobListingsData;
    if (selectedDepartment) {
        agencyFilterData = agencyFilterData.filter(row => row.Department === selectedDepartment);
    }
    const agencies = [...new Set(agencyFilterData
        .filter(row => row.Agency && row.Agency !== row.Department)
        .map(row => row.Agency)
    )].sort();
    
    // Rebuild appointment type dropdown (filtered by department and agency)
    let appointmentFilterData = jobListingsData;
    if (selectedDepartment) {
        appointmentFilterData = appointmentFilterData.filter(row => row.Department === selectedDepartment);
    }
    if (selectedAgency) {
        appointmentFilterData = appointmentFilterData.filter(row => row.Agency === selectedAgency);
    }
    
    const appointmentTypeMap = new Map();
    appointmentFilterData.forEach(row => {
        const apptType = row.Appointment_Type;
        const upperType = apptType ? apptType.toUpperCase() : '';
        // Exclude "All" and empty values
        if (!appointmentTypeMap.has(upperType) && apptType && apptType !== 'All') {
            appointmentTypeMap.set(upperType, apptType);
        }
    });
    const appointmentTypes = Array.from(appointmentTypeMap.values()).sort();
    
    // Rebuild occupation dropdown (filtered by all previous selections)
    const occupations = [...new Set(filteredData.map(row => row.Occupation_Series))]
        .filter(occ => occ && occ !== 'Unknown' && occ !== '*')
        .sort((a, b) => {
            // Sort by series number
            const aNum = parseInt(a) || 9999;
            const bNum = parseInt(b) || 9999;
            return aNum - bNum;
        });
    
    // Repopulate dropdowns
    populateDropdown('mainAgencyFilter', agencies);
    populateDropdown('mainAppointmentFilter', appointmentTypes);
    populateOccupationDropdown('mainOccupationFilter', occupations);
    
    // Restore selections if they're still valid
    if (agencies.includes(currentAgency)) {
        $('#mainAgencyFilter').val(currentAgency);
    }
    if (appointmentTypes.includes(currentAppointment)) {
        $('#mainAppointmentFilter').val(currentAppointment);
    }
    if (occupations.includes(currentOccupation)) {
        $('#mainOccupationFilter').val(currentOccupation);
    }
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

// Populate occupation dropdown with series and names
function populateOccupationDropdown(selectId, options) {
    const $select = $(`#${selectId}`);
    const currentValue = $select.val();
    
    $select.empty();
    $select.append('<option value="">All Occupations</option>');
    
    options.forEach((series, index) => {
        // The new map should already have both formats
        let name = occupationSeriesMap[series] || '';
        
        // Format the name properly (title case instead of all caps)
        if (name) {
            name = name.toLowerCase()
                .split(' ')
                .map(word => word.charAt(0).toUpperCase() + word.slice(1))
                .join(' ');
        }
        
        const displayText = name ? `${series} - ${name}` : series;
        $select.append(`<option value="${series}">${displayText}</option>`);
    });
    
    // Restore previous selection if it exists
    if (currentValue && options.includes(currentValue)) {
        $select.val(currentValue);
    }
}

// Initialize DataTable
function initializeDataTable(includeSubagency = false) {
    // Define base columns
    const columns = [
        { 
            data: 'Department',
            render: function(data) {
                const color = DEPARTMENT_COLORS[data] || '#999999';
                return `<span style="display: inline-block; width: 10px; height: 10px; background-color: ${color}; border-radius: 50%; margin-right: 8px;"></span>${data}`;
            }
        },
        { data: 'Agency' }
    ];
    
    // Add subagency column if needed
    if (includeSubagency) {
        columns.push({ data: 'Subagency' });
    }
    
    // Add numeric columns
    columns.push(
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
                const className = data >= 90 ? 'high-percentage' :    // Green if maintained most hiring
                                data >= 50 ? 'medium-percentage' :    // Orange if moderate decline
                                'low-percentage';                      // Red if major decline
                return `<span class="percentage-cell ${className}">${formatPercentage(data)}</span>`;
            }
        }
    );
    
    // Initialize with empty data - will be populated by updateTableData
    dataTable = $('#jobsTable').DataTable({
        data: [],
        columns: columns,
        order: [[includeSubagency ? 3 : 2, 'desc']], // Sort by 2024 listings
        pageLength: 25,
        lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "All"]],
        responsive: true,
        dom: 'lfrtip'
    });
    
    // Show appropriate data based on aggregation level
    updateTableData();
}

// Update table data based on current filters and aggregation level
async function updateTableData() {
    if (!dataTable) return;
    
    let dataToShow;
    if (aggregationLevel === 'department') {
        dataToShow = getFilteredDepartmentData().map(d => ({
            Department: d.name,
            Agency: `${d.agencyCount} agencies`,
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
    } else if (aggregationLevel === 'subagency') {
        // Handle subagency view
        const deptFilter = $('#mainDepartmentFilter').val();
        if (!deptFilter || !currentDepartmentData) {
            // No department selected or data not loaded
            dataToShow = [];
        } else {
            dataToShow = getFilteredSubagencyData().map(d => ({
                Department: d.department,
                Agency: d.agency,
                Subagency: d.name,
                listings2024: d.listings2024,
                listings2025: d.listings2025,
                percentageOf2024: d.percentageOf2024
            }));
        }
    } else {
        // Individual records with filters applied
        dataToShow = jobListingsData.filter(row => {
            const deptFilter = $('#mainDepartmentFilter').val();
            if (deptFilter && row.Department !== deptFilter) return false;
            
            const agencyFilter = $('#mainAgencyFilter').val();
            if (agencyFilter && row.Agency !== agencyFilter) return false;
            
            const apptFilter = $('#mainAppointmentFilter').val();
            if (apptFilter && row.Appointment_Type !== apptFilter) return false;
            
            const occupationFilter = $('#mainOccupationFilter').val();
            if (occupationFilter && row.Occupation_Series !== occupationFilter) return false;
            
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
    } else if (aggregationLevel === 'subagency') {
        dataToShow = getFilteredSubagencyData();
    } else {
        dataToShow = getFilteredAgencyData();
    }
    
    // Filter out entities with no 2024 listings
    dataToShow = dataToShow.filter(d => d.listings2024 > 0);
    
    // Create scales - dynamically based on actual data
    const xMax = d3.max(dataToShow, d => d.percentageOf2024) || 100;
    const xScale = d3.scaleLinear()
        .domain([-5, Math.max(100, xMax * 1.1)]) // Start at -5 to give space for 0% bubbles
        .range([0, width]);
    
    // Size scale for bubbles
    const maxListings = d3.max(dataToShow, d => d.listings2024);
    const sizeScale = d3.scaleSqrt()
        .domain([0, maxListings])
        .range([2, 80]);
    
    // Create force simulation with boundary constraints
    const simulation = d3.forceSimulation(dataToShow)
        .force('x', d3.forceX(d => xScale(d.percentageOf2024)).strength(1))
        .force('y', d3.forceY(height / 2).strength(0.05))  // Reduced strength for more vertical spread
        .force('collide', d3.forceCollide(d => sizeScale(d.listings2024) + 3))  // Increased padding between bubbles
        .stop();
    
    // Run simulation with boundary constraints
    for (let i = 0; i < 200; ++i) {  // Increased iterations for better positioning
        simulation.tick();
        
        // Constrain bubbles to stay within bounds
        dataToShow.forEach(d => {
            const radius = sizeScale(d.listings2024);
            // Keep bubbles within x bounds
            d.x = Math.max(radius, Math.min(width - radius, d.x));
            // Keep bubbles within y bounds
            d.y = Math.max(radius, Math.min(height - radius, d.y));
        });
    }
    
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
        
        // Position text to avoid cutoff
        const textX = Math.min(xScale(100) + 5, width - 70); // Ensure text doesn't go past right edge
        g.append('text')
            .attr('x', textX)
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
            } else if (aggregationLevel === 'subagency') {
                content = `
                    <strong>${d.name}</strong><br/>
                    Agency: ${d.agency}<br/>
                    Department: ${d.department}<br/>
                    2024 Listings: ${formatNumber(d.listings2024)}<br/>
                    2025 Listings: ${formatNumber(d.listings2025)}<br/>
                    2025 as % of 2024: ${formatPercentage(d.percentageOf2024)}
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
        
        const occupationFilter = $('#mainOccupationFilter').val();
        if (occupationFilter && row.Occupation_Series !== occupationFilter) return false;
        
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

// Get subagency data from department-specific data
function getFilteredSubagencyData() {
    if (!currentDepartmentData) return [];
    
    // Apply filters to department data
    const filteredRaw = currentDepartmentData.filter(row => {
        const agencyFilter = $('#mainAgencyFilter').val();
        if (agencyFilter && row.Agency !== agencyFilter) return false;
        
        const apptFilter = $('#mainAppointmentFilter').val();
        if (apptFilter && row.Appointment_Type !== apptFilter) return false;
        
        const occupationFilter = $('#mainOccupationFilter').val();
        if (occupationFilter && row.Occupation_Series !== occupationFilter) return false;
        
        return true;
    });
    
    // Re-aggregate by subagency
    const subagencyMap = new Map();
    filteredRaw.forEach(row => {
        const key = `${row.Department}|${row.Agency}|${row.Subagency}`;
        if (!subagencyMap.has(key)) {
            subagencyMap.set(key, {
                department: row.Department,
                agency: row.Agency,
                name: row.Subagency,
                listings2024: 0,
                listings2025: 0
            });
        }
        const data = subagencyMap.get(key);
        data.listings2024 += row.listings2024Value;
        data.listings2025 += row.listings2025Value;
    });
    
    // Convert to array and calculate percentages
    return Array.from(subagencyMap.values()).map(data => {
        data.percentageOf2024 = data.listings2024 > 0 ? 
            (data.listings2025 / data.listings2024 * 100) : 0;
        return data;
    });
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
        
        const occupationFilter = $('#mainOccupationFilter').val();
        if (occupationFilter && row.Occupation_Series !== occupationFilter) return false;
        
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


// Update the active filters display under the title
function updateActiveFiltersDisplay() {
    const $display = $('#activeFiltersDisplay');
    const filterTexts = [];
    
    // Get selected values
    const dept = $('#mainDepartmentFilter').val();
    const agency = $('#mainAgencyFilter').val();
    const apptType = $('#mainAppointmentFilter').val();
    const occupation = $('#mainOccupationFilter').val();
    
    // Only show if filters are applied
    if (dept || agency || apptType || occupation) {
        if (dept) filterTexts.push(`Department: ${dept}`);
        if (agency) filterTexts.push(`Agency: ${agency}`);
        if (apptType) filterTexts.push(`Type: ${apptType}`);
        if (occupation) filterTexts.push(`Occupation: ${occupation}`);
        
        $display.text('Filtered by: ' + filterTexts.join(', '));
    } else {
        $display.text('');
    }
}

// Initialize event handlers
function initializeEventHandlers() {
    // Department filter changes - update dependent filters
    $('#mainDepartmentFilter').on('change', async function() {
        console.log('Department filter changed:', $(this).val());
        const selectedDept = $(this).val();
        
        // Handle subagency option availability
        const $subagencyOption = $('#aggregationLevel option[value="subagency"]');
        if (selectedDept) {
            // Enable subagency option and update text
            $subagencyOption.prop('disabled', false).text('Subagency');
            
            // If currently viewing subagency, load department data
            if (aggregationLevel === 'subagency') {
                currentDepartmentData = await loadDepartmentData(selectedDept);
            }
        } else {
            // Disable subagency option
            $subagencyOption.prop('disabled', true).text('Subagency (select a department first)');
            
            // If was viewing subagency, switch back to agency view
            if (aggregationLevel === 'subagency') {
                $('#aggregationLevel').val('agency').trigger('change');
            }
            currentDepartmentData = null;
        }
        
        updateDependentFilters();
        updateTableData();
        updateFilteredStats();
        updateActiveFiltersDisplay();
        initializeBubbleChart();
    });
    
    // Agency filter changes - update appointment and occupation filters
    $('#mainAgencyFilter').on('change', function() {
        console.log('Agency filter changed:', $(this).val());
        updateDependentFilters();
        updateTableData();
        updateFilteredStats();
        updateActiveFiltersDisplay();
        initializeBubbleChart();
    });
    
    // Appointment filter changes - update occupation filter
    $('#mainAppointmentFilter').on('change', function() {
        console.log('Appointment filter changed:', $(this).val());
        updateDependentFilters();
        updateTableData();
        updateFilteredStats();
        updateActiveFiltersDisplay();
        initializeBubbleChart();
    });
    
    // Occupation filter changes - just update display
    $('#mainOccupationFilter').on('change', function() {
        console.log('Occupation filter changed:', $(this).val());
        updateTableData();
        updateFilteredStats();
        updateActiveFiltersDisplay();
        initializeBubbleChart();
    });
    
    // Aggregation level change
    $('#aggregationLevel').on('change', async function() {
        aggregationLevel = $(this).val();
        
        // If switching to subagency view, load department data if needed
        if (aggregationLevel === 'subagency') {
            const selectedDept = $('#mainDepartmentFilter').val();
            if (selectedDept && !currentDepartmentData) {
                currentDepartmentData = await loadDepartmentData(selectedDept);
            }
            
            // Destroy existing DataTable
            if (dataTable) {
                dataTable.destroy();
                $('#jobsTable').empty(); // Clear the entire table
            }
            
            // Recreate table structure with subagency column
            $('#jobsTable').html(`
                <thead>
                    <tr>
                        <th>Department</th>
                        <th>Agency</th>
                        <th>Subagency</th>
                        <th>2024 Listings (Feb-Aug)</th>
                        <th>2025 Listings (Feb-Aug)</th>
                        <th>Change</th>
                        <th>2025 as % of 2024</th>
                    </tr>
                </thead>
                <tbody></tbody>
            `);
            
            // Initialize DataTable with subagency column
            initializeDataTable(true); // true = include subagency column
        } else {
            // Destroy existing DataTable
            if (dataTable) {
                dataTable.destroy();
                $('#jobsTable').empty(); // Clear the entire table
            }
            
            // Recreate table structure without subagency column
            $('#jobsTable').html(`
                <thead>
                    <tr>
                        <th>Department</th>
                        <th>Agency</th>
                        <th>2024 Listings (Feb-Aug)</th>
                        <th>2025 Listings (Feb-Aug)</th>
                        <th>Change</th>
                        <th>2025 as % of 2024</th>
                    </tr>
                </thead>
                <tbody></tbody>
            `);
            
            // Initialize DataTable without subagency column
            initializeDataTable(false); // false = no subagency column
        }
        
        updateTableData();
        initializeBubbleChart();
        
        // Update entity label
        const label = aggregationLevel === 'department' ? 'Departments' : 
                      aggregationLevel === 'agency' ? 'Agencies' : 'Subagencies';
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
    updateActiveFiltersDisplay();
});