/* =============================================================================
   Retirement Research Digest — App
   ============================================================================= */

(function () {
  'use strict';

  // --- Config ---
  const PAPERS_PER_PAGE = 30;
  const NEW_BADGE_DAYS = 3;
  const DEBOUNCE_MS = 250;

  const TOPIC_LABELS = {
    public_pensions:     'Public Pensions / SS',
    consumption_savings: 'Consumption-Savings',
    social_insurance:    'Social Insurance',
    retirement_decisions:'Retirement Decisions',
    longevity_mortality: 'Longevity / Mortality',
    private_pensions:    'Private Pensions',
    insurance_markets:   'Annuities & Insurance',
    health_retirement:   'Health Care & Medicare',
  };

  // --- State ---
  let allPapers = [];
  let meta = {};
  let filteredPapers = [];
  let displayedCount = 0;
  let activeTopics = new Set();
  let searchQuery = '';
  let timeDays = 30;

  // --- DOM refs ---
  const searchInput    = document.getElementById('searchInput');
  const topicPillsEl   = document.getElementById('topicPills');
  const timeFilter     = document.getElementById('timeFilter');
  const statusText     = document.getElementById('statusText');
  const filterLogicEl  = document.getElementById('filterLogicHint');
  const clearFiltersEl = document.getElementById('clearFilters');
  const paperList      = document.getElementById('paperList');
  const loadMoreDiv    = document.getElementById('loadMore');
  const loadMoreBtn    = document.getElementById('loadMoreBtn');
  const noPapersEl     = document.getElementById('noPapers');

  // --- Init ---
  async function init() {
    try {
      const resp = await fetch('data/papers.json');
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      allPapers = data.papers || [];
      meta = data.meta || {};
    } catch (err) {
      statusText.textContent = 'Failed to load papers. Please try again later.';
      console.error('Failed to load papers:', err);
      return;
    }

    buildTopicPills();
    bindEvents();
    applyFilters();
  }

  // --- Topic Pills ---
  function buildTopicPills() {
    const counts = {};
    for (const p of allPapers) {
      for (const t of p.topics) {
        counts[t] = (counts[t] || 0) + 1;
      }
    }

    for (const [slug, label] of Object.entries(TOPIC_LABELS)) {
      const count = counts[slug] || 0;
      if (count === 0) continue; // hide empty topics
      const pill = document.createElement('button');
      pill.className = 'topic-pill';
      pill.dataset.topic = slug;
      pill.textContent = `${label} (${count})`;
      pill.setAttribute('aria-pressed', 'false');
      pill.addEventListener('click', () => toggleTopic(slug, pill));
      topicPillsEl.appendChild(pill);
    }
  }

  function toggleTopic(slug, pill) {
    if (activeTopics.has(slug)) {
      activeTopics.delete(slug);
      pill.classList.remove('active');
      pill.setAttribute('aria-pressed', 'false');
    } else {
      activeTopics.add(slug);
      pill.classList.add('active');
      pill.setAttribute('aria-pressed', 'true');
    }
    applyFilters();
  }

  // --- Events ---
  function bindEvents() {
    searchInput.addEventListener('input', debounce(() => {
      searchQuery = searchInput.value.trim().toLowerCase();
      applyFilters();
    }, DEBOUNCE_MS));

    timeFilter.addEventListener('change', () => {
      timeDays = timeFilter.value === 'all' ? null : parseInt(timeFilter.value, 10);
      applyFilters();
    });

    loadMoreBtn.addEventListener('click', () => {
      renderMore();
    });

    clearFiltersEl.addEventListener('click', () => {
      activeTopics.clear();
      searchQuery = '';
      timeDays = 30;
      searchInput.value = '';
      timeFilter.value = '30';
      topicPillsEl.querySelectorAll('.topic-pill').forEach(p => {
        p.classList.remove('active');
        p.setAttribute('aria-pressed', 'false');
      });
      applyFilters();
    });

    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
      if (e.key === '/' && document.activeElement !== searchInput) {
        e.preventDefault();
        searchInput.focus();
      }
      if (e.key === 'Escape' && document.activeElement === searchInput) {
        searchInput.value = '';
        searchQuery = '';
        searchInput.blur();
        applyFilters();
      }
    });
  }

  // --- Filtering ---
  function applyFilters() {
    const now = new Date();

    filteredPapers = allPapers.filter(p => {
      // Time filter
      if (timeDays !== null) {
        const pubDate = new Date(p.publication_date);
        const diffDays = (now - pubDate) / (1000 * 60 * 60 * 24);
        if (diffDays > timeDays) return false;
      }

      // Topic filter (OR across selected topics, AND with search)
      if (activeTopics.size > 0) {
        const hasMatch = p.topics.some(t => activeTopics.has(t));
        if (!hasMatch) return false;
      }

      // Text search
      if (searchQuery) {
        const searchable = [
          p.title,
          p.authors.map(a => a.name).join(' '),
          p.abstract,
        ].join(' ').toLowerCase();
        if (!searchable.includes(searchQuery)) return false;
      }

      return true;
    });

    // Sort by publication date descending
    filteredPapers.sort((a, b) => b.publication_date.localeCompare(a.publication_date));

    displayedCount = 0;
    paperList.innerHTML = '';
    renderMore();
    updateStatus();
  }

  // --- Rendering ---
  function renderMore() {
    const end = Math.min(displayedCount + PAPERS_PER_PAGE, filteredPapers.length);

    for (let i = displayedCount; i < end; i++) {
      paperList.appendChild(createPaperCard(filteredPapers[i]));
    }

    displayedCount = end;

    // Show/hide load more with remaining count
    if (displayedCount < filteredPapers.length) {
      const remaining = filteredPapers.length - displayedCount;
      loadMoreBtn.textContent = `Load ${Math.min(PAPERS_PER_PAGE, remaining)} more (${remaining} remaining)`;
      loadMoreDiv.style.display = 'block';
    } else {
      loadMoreDiv.style.display = 'none';
    }
    noPapersEl.style.display = filteredPapers.length === 0 ? 'block' : 'none';
  }

  function createPaperCard(paper) {
    const card = document.createElement('div');
    card.className = 'paper-card';

    const isNew = isRecentlyAdded(paper);
    const dateStr = formatDate(paper.publication_date);
    const authorsStr = formatAuthors(paper.authors);

    // Build abstract HTML without inline handlers
    let abstractHtml = '';
    if (paper.abstract) {
      abstractHtml = `
        <div class="paper-abstract collapsed">${escapeHtml(paper.abstract)}</div>
        <button class="paper-abstract-toggle" tabindex="0" role="button" aria-expanded="false">Show more</button>
      `;
    }

    card.innerHTML = `
      <div class="paper-header">
        <div class="paper-title">
          <a href="${escapeHtml(paper.url)}" target="_blank" rel="noopener">${escapeHtml(paper.title)}</a>
          ${isNew ? '<span class="badge-new">NEW</span>' : ''}
        </div>
        <span class="paper-date">${dateStr}</span>
      </div>
      <div class="paper-meta">
        <span class="paper-authors">${escapeHtml(authorsStr)}</span>
        <span class="paper-source"> · ${escapeHtml(paper.source)}</span>
      </div>
      <div class="paper-topics">
        ${paper.topics.map(t => `<span class="paper-topic-tag">${escapeHtml(TOPIC_LABELS[t] || t)}</span>`).join('')}
      </div>
      ${abstractHtml}
    `;

    // Wire up abstract toggle with proper event listeners
    const toggleBtn = card.querySelector('.paper-abstract-toggle');
    const abstractDiv = card.querySelector('.paper-abstract');
    if (toggleBtn && abstractDiv) {
      const toggle = () => {
        abstractDiv.classList.toggle('collapsed');
        const expanded = !abstractDiv.classList.contains('collapsed');
        toggleBtn.textContent = expanded ? 'Show less' : 'Show more';
        toggleBtn.setAttribute('aria-expanded', String(expanded));
      };
      toggleBtn.addEventListener('click', toggle);
      toggleBtn.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); }
      });
    }

    return card;
  }

  function updateStatus() {
    const lastUpdated = meta.last_updated
      ? new Date(meta.last_updated).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
      : '';
    const totalStr = `${filteredPapers.length} paper${filteredPapers.length !== 1 ? 's' : ''}`;
    const updatedStr = lastUpdated ? ` · Last updated ${lastUpdated}` : '';
    statusText.textContent = `${totalStr}${updatedStr} · Newest first`;

    // Show logic hint when 2+ topics selected
    if (filterLogicEl) {
      filterLogicEl.style.display = activeTopics.size >= 2 ? 'inline' : 'none';
    }

    // Show clear filters button when any filter is active
    const hasFilters = activeTopics.size > 0 || searchQuery || timeDays !== 30;
    clearFiltersEl.style.display = hasFilters ? 'inline' : 'none';
  }

  // --- Helpers ---
  function isRecentlyAdded(paper) {
    if (!paper.added_date) return false;
    const added = new Date(paper.added_date);
    const now = new Date();
    return (now - added) / (1000 * 60 * 60 * 24) <= NEW_BADGE_DAYS;
  }

  function formatDate(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr + 'T00:00:00');
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  }

  function formatAuthors(authors) {
    if (!authors || authors.length === 0) return '';
    if (authors.length <= 3) {
      return authors.map(a => a.name).join(', ');
    }
    return `${authors[0].name}, ${authors[1].name}, ... +${authors.length - 2} more`;
  }

  function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  function debounce(fn, ms) {
    let timer;
    return function (...args) {
      clearTimeout(timer);
      timer = setTimeout(() => fn.apply(this, args), ms);
    };
  }

  // --- Start ---
  document.addEventListener('DOMContentLoaded', init);
})();
