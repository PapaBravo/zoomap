/* ── Constants ───────────────────────────────────────────────── */
const DATA_URL = 'data/animals.geojson';

// Tierpark Berlin centre
const MAP_CENTER = [52.5093, 13.5255];
const MAP_ZOOM = 16;

const COLOR_DEFAULT = '#2d6a2d';
const COLOR_SELECTED = '#e65c00';
const COLOR_POLYGON_DEFAULT_FILL = '#2d6a2d';
const COLOR_POLYGON_SELECTED_FILL = '#e65c00';

/* ── State ───────────────────────────────────────────────────── */
let selectedId = null;

// map from feature id → { listItem, layers[] }
const featureMap = {};

/* ── Map initialisation ──────────────────────────────────────── */
const map = L.map('map').setView(MAP_CENTER, MAP_ZOOM);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  maxZoom: 19,
}).addTo(map);

/* ── Helpers ─────────────────────────────────────────────────── */
function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function buildPopupHtml(props) {
  const img = props.image
    ? `<img src="${escapeHtml(props.image)}" alt="${escapeHtml(props.name)}" class="popup-image" />`
    : '';
  const species = props.species
    ? `<em>${escapeHtml(props.species)}</em>`
    : '';
  const enclosureTag = props.enclosure && props.enclosure !== props.name
    ? `<div class="popup-enclosure">📍 ${escapeHtml(props.enclosure)}</div>`
    : '';
  const desc = props.description
    ? `<p>${escapeHtml(props.description)}</p>`
    : '';
  const wikiLink = props.wikipedia
    ? `<a href="${escapeHtml(props.wikipedia)}" target="_blank" rel="noopener noreferrer" class="popup-wiki-link">Wikipedia ↗</a>`
    : '';
  return `${img}<strong>${escapeHtml(props.name)}</strong>${species}${enclosureTag}${desc}${wikiLink}`;
}

function defaultStyle() {
  return { color: COLOR_DEFAULT, fillColor: COLOR_POLYGON_DEFAULT_FILL, fillOpacity: 0.25, weight: 2 };
}

function selectedStyle() {
  return { color: COLOR_SELECTED, fillColor: COLOR_POLYGON_SELECTED_FILL, fillOpacity: 0.4, weight: 3 };
}

function markerIcon(color) {
  // Simple SVG pin coloured by state
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="24" height="36" viewBox="0 0 24 36">
    <path d="M12 0C5.373 0 0 5.373 0 12c0 9 12 24 12 24s12-15 12-24C24 5.373 18.627 0 12 0z" fill="${color}" stroke="#fff" stroke-width="1.5"/>
    <circle cx="12" cy="12" r="5" fill="#fff"/>
  </svg>`;
  return L.divIcon({
    html: svg,
    className: '',
    iconSize: [24, 36],
    iconAnchor: [12, 36],
    popupAnchor: [0, -36],
  });
}

/* ── Selection ───────────────────────────────────────────────── */
function selectFeature(id) {
  // Deselect previous
  if (selectedId !== null && featureMap[selectedId]) {
    const prev = featureMap[selectedId];
    prev.listItem.classList.remove('selected');
    prev.layers.forEach((l) => applyLayerStyle(l, false));
  }

  selectedId = id;

  if (id === null || !featureMap[id]) return;

  const entry = featureMap[id];
  entry.listItem.classList.add('selected');
  entry.listItem.scrollIntoView({ block: 'nearest' });
  entry.layers.forEach((l) => applyLayerStyle(l, true));

  // Pan / zoom: prefer polygon (getBounds), fall back to marker (getLatLng)
  const polygonLayer = entry.layers.find((l) => l.getBounds);
  const markerLayer = entry.layers.find((l) => l.getLatLng);
  if (polygonLayer) {
    map.fitBounds(polygonLayer.getBounds(), { maxZoom: 18 });
  } else if (markerLayer) {
    map.setView(markerLayer.getLatLng(), Math.max(map.getZoom(), 17));
  }

  // Open popup on marker if available, otherwise on first layer
  const popupTarget = markerLayer || entry.layers[0];
  popupTarget.openPopup();
}

function applyLayerStyle(layer, isSelected) {
  if (layer.setIcon) {
    layer.setIcon(markerIcon(isSelected ? COLOR_SELECTED : COLOR_DEFAULT));
  } else if (layer.setStyle) {
    layer.setStyle(isSelected ? selectedStyle() : defaultStyle());
  }
}

/* ── GeoJSON loading ─────────────────────────────────────────── */
fetch(DATA_URL)
  .then((res) => {
    if (!res.ok) throw new Error(`Failed to load ${DATA_URL}: ${res.status}`);
    return res.json();
  })
  .then((geojson) => {
    const listEl = document.getElementById('animal-list');

    // Deduplicate: one list entry per unique feature id
    const listedIds = new Set();

    const geoLayer = L.geoJSON(geojson, {
      pointToLayer(feature, latlng) {
        return L.marker(latlng, { icon: markerIcon(COLOR_DEFAULT) });
      },
      style() {
        return defaultStyle();
      },
      onEachFeature(feature, layer) {
        const props = feature.properties;
        const id = props.id;

        layer.bindPopup(buildPopupHtml(props));

        layer.on('click', () => selectFeature(id));

        // Create list item only once per id; accumulate all layers per id
        if (!listedIds.has(id)) {
          listedIds.add(id);

          const listItem = document.createElement('li');
          listItem.dataset.id = id;
          listItem.dataset.search = `${props.name} ${props.species || ''} ${props.description || ''} ${props.enclosure || ''}`.toLowerCase();

          const thumb = props.image
            ? `<img src="${escapeHtml(props.image)}" alt="${escapeHtml(props.name)}" class="animal-thumb" />`
            : '';
          listItem.innerHTML = `
            ${thumb}
            <div class="animal-info">
              <div class="animal-name">${escapeHtml(props.name)}</div>
              <div class="animal-species">${escapeHtml(props.species)}</div>
            </div>
          `;

          listItem.addEventListener('click', () => selectFeature(id));
          listEl.appendChild(listItem);

          featureMap[id] = { listItem, layers: [layer] };
        } else {
          // Accumulate additional layers (e.g. both a marker and a polygon enclosure)
          featureMap[id].layers.push(layer);
        }
      },
    }).addTo(map);

    map.fitBounds(geoLayer.getBounds(), { padding: [20, 20] });
  })
  .catch((err) => {
    console.error(err);
  });

/* ── Search ──────────────────────────────────────────────────── */
document.getElementById('search').addEventListener('input', function () {
  const query = this.value.trim().toLowerCase();
  const items = document.querySelectorAll('#animal-list li');
  items.forEach((li) => {
    const match = !query || li.dataset.search.includes(query);
    li.classList.toggle('hidden', !match);
  });
});
