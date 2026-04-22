/**
 * Blog Post Loader for Pro Truck Logistics
 * This script loads blog posts from the index.json file and displays them on the blog page
 */

document.addEventListener('DOMContentLoaded', function() {
    // Configuration
    const postsPerPage = 6;
    const blogPostsContainer = document.getElementById('blog-posts-container');
    const paginationContainer = document.getElementById('pagination-container');
    const categoriesContainer = document.getElementById('categories-container');
    const recentPostsContainer = document.getElementById('recent-posts-container');
    const tagsContainer = document.getElementById('tags-container');
    
    // Get the current page and category from URL parameters
    const urlParams = new URLSearchParams(window.location.search);
    let currentPage = parseInt(urlParams.get('page')) || 1;
    let currentCategory = urlParams.get('category') || null;
    
    // Fetch blog posts from the index.json file
    fetch('blog-posts/index.json')
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(posts => {
            // Sort posts by date (newest first)
            posts = sortPostsByDate(posts);
            
            // Display blog posts
            displayBlogPosts(posts);
            
            // Update sidebar
            updateSidebar(posts);
        })
        .catch(error => {
            console.error('Error fetching blog posts:', error);
            
            // Display an error message
            if (blogPostsContainer) {
                blogPostsContainer.innerHTML = `
                    <div class="alert alert-danger">
                        <h4>Error Loading Blog Posts</h4>
                        <p>We're sorry, but there was an error loading the blog posts. Please try again later.</p>
                    </div>
                `;
            }
        });
    
    /**
     * Sort posts by date (newest first)
     */
    function sortPostsByDate(posts) {
        return posts.sort((a, b) => {
            // Parse dates (assuming MM/DD/YYYY or similar format)
            const dateA = new Date(a.date);
            const dateB = new Date(b.date);
            
            // Sort newest first
            return dateB - dateA;
        });
    }
    
    /**
     * Display blog posts with pagination and category filtering
     */
    function displayBlogPosts(posts) {
        if (!blogPostsContainer) return;
        
        // Filter posts by category if a category is selected
        let filteredPosts = posts;
        if (currentCategory) {
            filteredPosts = posts.filter(post => post.category === currentCategory);
        }
        
        // Calculate pagination
        const totalPosts = filteredPosts.length;
        const totalPages = Math.ceil(totalPosts / postsPerPage);
        
        // Ensure current page is valid
        if (currentPage < 1) currentPage = 1;
        if (currentPage > totalPages) currentPage = totalPages;
        
        // Calculate start and end indices for current page
        const startIndex = (currentPage - 1) * postsPerPage;
        const endIndex = startIndex + postsPerPage;
        const postsToDisplay = filteredPosts.slice(startIndex, endIndex);
        
        // Clear the container
        blogPostsContainer.innerHTML = '';
        
        // Display category filter message if filtering
        if (currentCategory) {
            blogPostsContainer.innerHTML = `
                <div class="alert alert-info mb-4">
                    <h5 class="mb-0">
                        <i class="fas fa-filter me-2"></i> Showing posts in category: "${currentCategory}" 
                        <a href="?page=1" class="btn btn-sm btn-outline-primary ms-3">Show All Categories</a>
                    </h5>
                </div>
            `;
        }
        
        // Display posts for the current page
        if (postsToDisplay.length === 0) {
            blogPostsContainer.innerHTML += `
                <div class="alert alert-info">
                    <h4>No Blog Posts Found</h4>
                    <p>No posts available ${currentCategory ? `in the "${currentCategory}" category` : ''}. Check back soon for new content!</p>
                </div>
            `;
        } else {
            postsToDisplay.forEach(post => {
                blogPostsContainer.innerHTML += `
                    <div class="blog-card">
                        <div class="blog-image" style="background-image: url('${post.image}');"></div>
                        <div class="blog-content">
                            <span class="blog-category">${post.category}</span>
                            <h3 class="blog-title">${post.title}</h3>
                            <p class="blog-date"><i class="far fa-calendar-alt me-2"></i>${post.date}</p>
                            <p class="blog-excerpt">${post.excerpt}</p>
                            <a href="blog-posts/post-${post.id}.html" class="btn btn-primary">Read More</a>
                        </div>
                    </div>
                `;
            });
        }
        
        // Display pagination
        updatePagination(totalPages, currentPage);
    }
    
    /**
     * Update pagination links, preserving category filter
     */
    function updatePagination(totalPages, currentPage) {
        if (!paginationContainer) return;
        
        // Clear the container
        paginationContainer.innerHTML = '';
        
        // No pagination needed if only one page
        if (totalPages <= 1) return;
        
        // Build the base URL with category if present
        let baseUrl = '?';
        if (currentCategory) {
            baseUrl += `category=${encodeURIComponent(currentCategory)}&`;
        }
        
        // Previous page
        if (currentPage > 1) {
            paginationContainer.innerHTML += `
                <li class="page-item">
                    <a class="page-link" href="${baseUrl}page=${currentPage - 1}" aria-label="Previous">
                        <span aria-hidden="true">&laquo;</span>
                    </a>
                </li>
            `;
        }
        
        // Page numbers
        for (let i = 1; i <= totalPages; i++) {
            paginationContainer.innerHTML += `
                <li class="page-item ${i === currentPage ? 'active' : ''}">
                    <a class="page-link" href="${baseUrl}page=${i}">${i}</a>
                </li>
            `;
        }
        
        // Next page
        if (currentPage < totalPages) {
            paginationContainer.innerHTML += `
                <li class="page-item">
                    <a class="page-link" href="${baseUrl}page=${currentPage + 1}" aria-label="Next">
                        <span aria-hidden="true">&raquo;</span>
                    </a>
                </li>
            `;
        }
    }
    
    /**
     * Update sidebar with categories, recent posts, and tags
     */
    function updateSidebar(posts) {
        // Update categories
        if (categoriesContainer) {
            // Get unique categories and count occurrences
            const categories = {};
            posts.forEach(post => {
                if (!categories[post.category]) {
                    categories[post.category] = 0;
                }
                categories[post.category]++;
            });
            
            // Clear the container
            categoriesContainer.innerHTML = '';
            
            // Add "All Categories" option at the top
            categoriesContainer.innerHTML += `
                <li><a href="?page=1" class="fw-bold">All Categories (${posts.length})</a></li>
            `;
            
            // Display categories
            Object.keys(categories).sort().forEach(category => {
                // Highlight the current category
                const isActive = category === currentCategory;
                categoriesContainer.innerHTML += `
                    <li><a href="?category=${encodeURIComponent(category)}&page=1" ${isActive ? 'class="fw-bold text-secondary-custom"' : ''}>${category} (${categories[category]})</a></li>
                `;
            });
        }
        
        // Update recent posts
        if (recentPostsContainer) {
            // Sort posts by date (most recent first)
            const recentPosts = [...posts].slice(0, 4);
            
            // Clear the container
            recentPostsContainer.innerHTML = '';
            
            // Display recent posts
            recentPosts.forEach(post => {
                recentPostsContainer.innerHTML += `
                    <li>
                        <a href="blog-posts/post-${post.id}.html">${post.title}</a>
                        <p class="blog-date"><i class="far fa-calendar-alt me-2"></i>${post.date}</p>
                    </li>
                `;
            });
        }
        
        // Update tags
        if (tagsContainer) {
            // Extract keywords from posts and count occurrences
            const allTags = [];
            posts.forEach(post => {
                // Each post might have tags/keywords in its meta data
                // For simplicity, we'll just use categories as tags
                allTags.push(post.category);
            });
            
            // Get unique tags
            const uniqueTags = [...new Set(allTags)];
            
            // Clear the container
            tagsContainer.innerHTML = '';
            
            // Add "All" tag
            tagsContainer.innerHTML += `
                <a href="?page=1" class="btn ${!currentCategory ? 'btn-primary' : 'btn-outline-primary'} btn-sm m-1">All</a>
            `;
            
            // Display tags
            uniqueTags.forEach(tag => {
                // Highlight the current tag
                const isActive = tag === currentCategory;
                tagsContainer.innerHTML += `
                    <a href="?category=${encodeURIComponent(tag)}&page=1" class="btn ${isActive ? 'btn-primary' : 'btn-outline-primary'} btn-sm m-1">${tag}</a>
                `;
            });
        }
    }
});
