document.addEventListener("DOMContentLoaded", () => {
    const opportunityClasses = [
        "opportunity-very-low",
        "opportunity-low",
        "opportunity-medium",
        "opportunity-high",
        "opportunity-very-high",
        "opportunity-extreme",
        "opportunity-watchlist",
        "opportunity-elevated",
    ];
    const refreshIntervalMs = 4000;
    const metricLabels = {
        score: "Score",
        viewers: "Viewers",
        streams: "Streams",
        ratio: "Viewer / Stream",
        growth: "Growth",
        viewer_change: "Viewer Change",
    };
    const svgNamespace = "http://www.w3.org/2000/svg";
    let graphState = {
        metric: null,
        range: "live",
        customStart: "",
        customEnd: "",
    };

    const easeOutCubic = (value) => 1 - ((1 - value) ** 3);

    const formatValue = (field, value) => {
        if (!Number.isFinite(value)) {
            return "";
        }

        switch (field) {
            case "score":
                return value.toLocaleString(undefined, {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 2,
                });
            case "viewers":
            case "streams":
            case "tracked_count":
                return Math.round(value).toLocaleString();
            case "ratio":
                return value.toLocaleString(undefined, {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 2,
                });
            case "growth":
                return `${value.toLocaleString(undefined, {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 2,
                })}%`;
            case "viewer_change":
                return `${value >= 0 ? "+" : "-"}${Math.abs(Math.round(value)).toLocaleString()}`;
            default:
                return value.toLocaleString(undefined, {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 2,
                });
        }
    };

    const formatDeltaValue = (field, value) => {
        if (!Number.isFinite(value) || Math.abs(value) < 0.001) {
            return "";
        }

        const sign = value > 0 ? "+" : "-";
        const absValue = Math.abs(value);

        switch (field) {
            case "score":
            case "ratio":
            case "growth":
                return `${sign}${absValue.toLocaleString(undefined, {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 2,
                })}`;
            case "viewers":
            case "streams":
            case "viewer_change":
            case "tracked_count":
                return `${sign}${Math.round(absValue).toLocaleString()}`;
            default:
                return `${sign}${absValue.toLocaleString(undefined, {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 2,
                })}`;
        }
    };

    const parseValue = (element, fallback = 0) => {
        const raw = element?.dataset?.value;
        const value = Number(raw);
        return Number.isFinite(value) ? value : fallback;
    };

    const parseVisibleValue = (element, field, fallback = 0) => {
        if (!element) {
            return fallback;
        }

        const text = (element.textContent || "").trim().replace(/,/g, "");
        if (!text) {
            return fallback;
        }

        let normalized = text;
        if (field === "growth") {
            normalized = normalized.replace("%", "");
        } else if (field === "viewer_change") {
            normalized = normalized.replace("+", "");
        }

        const parsed = Number(normalized);
        return Number.isFinite(parsed) ? parsed : fallback;
    };

    const setTrendDirection = (element, direction) => {
        if (!element) {
            return;
        }

        element.classList.remove("value-trend-up", "value-trend-down");
        if (direction > 0) {
            element.classList.add("value-trend-up");
            element.dataset.trendDirection = "up";
        } else if (direction < 0) {
            element.classList.add("value-trend-down");
            element.dataset.trendDirection = "down";
        } else if (element.dataset.preserveTrend === "true" && element.dataset.trendDirection === "up") {
            element.classList.add("value-trend-up");
        } else if (element.dataset.preserveTrend === "true" && element.dataset.trendDirection === "down") {
            element.classList.add("value-trend-down");
        } else {
            delete element.dataset.trendDirection;
        }
    };

    const pulseLiveElement = (element, direction) => {
        if (!element) {
            return;
        }

        element.classList.remove("live-value-up", "live-value-down");
        if (direction === 0) {
            return;
        }

        setTrendDirection(element, direction);
        element.classList.add(direction > 0 ? "live-value-up" : "live-value-down");
        window.clearTimeout(element._pulseTimeout);
        element._pulseTimeout = window.setTimeout(() => {
            element.classList.remove("live-value-up", "live-value-down");
        }, 900);
    };

    const pulseOpportunityElement = (element) => {
        if (!element) {
            return;
        }

        element.classList.remove("opportunity-refresh");
        void element.offsetWidth;
        element.classList.add("opportunity-refresh");
        window.clearTimeout(element._opportunityPulseTimeout);
        element._opportunityPulseTimeout = window.setTimeout(() => {
            element.classList.remove("opportunity-refresh");
        }, 900);
    };

    const animateNumericElement = (element, field, nextValue) => {
        if (!element || !Number.isFinite(nextValue)) {
            return;
        }

        const startValue = parseVisibleValue(
            element,
            field,
            parseValue(element, nextValue),
        );
        const movementDirection = Math.sign(nextValue - startValue);
        const baselineRaw = Number(element.dataset.baseline);
        const hasBaseline = Number.isFinite(baselineRaw);
        let trendDirection = hasBaseline ? Math.sign(nextValue - baselineRaw) : movementDirection;

        if (field === "growth" || field === "viewer_change") {
            trendDirection = Math.sign(nextValue);
        }

        element.dataset.preserveTrend = hasBaseline ? "false" : "true";
        setTrendDirection(element, trendDirection);

        if (Math.abs(nextValue - startValue) < 0.001) {
            element.dataset.value = String(nextValue);
            element.textContent = formatValue(field, nextValue);
            return;
        }

        if (element._animationFrame) {
            window.cancelAnimationFrame(element._animationFrame);
        }

        pulseLiveElement(element, movementDirection);
        setTrendDirection(element, trendDirection);

        const startTime = performance.now();
        const duration = 2200;

        const tick = (now) => {
            const progress = Math.min((now - startTime) / duration, 1);
            const eased = easeOutCubic(progress);
            const currentValue = startValue + ((nextValue - startValue) * eased);

            element.dataset.value = String(currentValue);
            element.textContent = formatValue(field, currentValue);

            if (progress < 1) {
                element._animationFrame = window.requestAnimationFrame(tick);
            } else {
                element.dataset.value = String(nextValue);
                element.textContent = formatValue(field, nextValue);
            }
        };

        element._animationFrame = window.requestAnimationFrame(tick);
    };

    const initializeBaselineTrendColors = () => {
        document.querySelectorAll('.gallery-inline-badge [data-live-field="score"], .gallery-inline-badge [data-live-field="viewers"]').forEach((element) => {
            const field = element.dataset.liveField || "";
            const value = parseValue(element, parseVisibleValue(element, field, 0));
            const baseline = Number(element.dataset.baseline);

            if (!Number.isFinite(value) || !Number.isFinite(baseline)) {
                return;
            }

            element.dataset.preserveTrend = "false";
            setTrendDirection(element, Math.sign(value - baseline));
        });

        document.querySelectorAll('[data-live-field="growth"], [data-live-field="viewer_change"]').forEach((element) => {
            const field = element.dataset.liveField || "";
            const value = parseValue(element, parseVisibleValue(element, field, 0));
            element.dataset.preserveTrend = "false";
            setTrendDirection(element, Math.sign(value));
        });
    };

    const syncDeltaIndicator = (scope, field, delta) => {
        if (!scope) {
            return;
        }

        const indicator = scope.querySelector(`[data-live-delta-for="${field}"]`);
        if (!indicator) {
            return;
        }

        const label = formatDeltaValue(field, delta);
        if (!label) {
            if (indicator.dataset.lastDeltaLabel) {
                indicator.textContent = indicator.dataset.lastDeltaLabel;
                indicator.classList.add("is-visible");
                if (indicator.dataset.lastDeltaDirection === "up") {
                    indicator.classList.add("is-up");
                    indicator.classList.remove("is-down");
                } else if (indicator.dataset.lastDeltaDirection === "down") {
                    indicator.classList.add("is-down");
                    indicator.classList.remove("is-up");
                }
            }
            return;
        }

        indicator.textContent = label;
        indicator.classList.remove("is-up", "is-down", "is-visible");
        indicator.dataset.lastDeltaLabel = label;
        indicator.dataset.lastDeltaDirection = delta > 0 ? "up" : "down";

        indicator.classList.add("is-visible", delta > 0 ? "is-up" : "is-down");
    };

    const getHistoryEntries = () => {
        const historyNode = document.querySelector("#history-data");
        if (!historyNode) {
            return [];
        }

        try {
            return JSON.parse(historyNode.textContent || "[]");
        } catch (error) {
            return [];
        }
    };

    const getHistorySummary = () => {
        const summaryNode = document.querySelector("#history-summary-data");
        if (!summaryNode) {
            return { count: 0, start: null, end: null };
        }

        try {
            return JSON.parse(summaryNode.textContent || "{}");
        } catch (error) {
            return { count: 0, start: null, end: null };
        }
    };

    const getHistoryBounds = (entries = getHistoryEntries()) => {
        const timestamps = entries
            .map((entry) => Date.parse(entry.timestamp))
            .filter((timestamp) => Number.isFinite(timestamp))
            .sort((left, right) => left - right);

        if (timestamps.length === 0) {
            return { min: null, max: null };
        }

        return {
            min: timestamps[0],
            max: timestamps[timestamps.length - 1],
        };
    };

    const toDatetimeLocalValue = (timestamp) => {
        if (!Number.isFinite(timestamp)) {
            return "";
        }

        const date = new Date(timestamp);
        const pad = (value) => String(value).padStart(2, "0");
        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
    };

    const filterHistoryEntries = (entries) => {
        const now = Date.now();
        let filtered = [...entries];

        if (graphState.range === "1h") {
            filtered = filtered.filter((entry) => now - Date.parse(entry.timestamp) <= 3600000);
        } else if (graphState.range === "6h") {
            filtered = filtered.filter((entry) => now - Date.parse(entry.timestamp) <= 21600000);
        } else if (graphState.range === "24h") {
            filtered = filtered.filter((entry) => now - Date.parse(entry.timestamp) <= 86400000);
        } else if (graphState.range === "custom") {
            const start = graphState.customStart ? Date.parse(graphState.customStart) : null;
            const end = graphState.customEnd ? Date.parse(graphState.customEnd) : null;
            filtered = filtered.filter((entry) => {
                const timestamp = Date.parse(entry.timestamp);
                if (start && timestamp < start) {
                    return false;
                }
                if (end && timestamp > end) {
                    return false;
                }
                return true;
            });
        }

        return filtered.filter((entry) => entry.timestamp && Number.isFinite(Date.parse(entry.timestamp)));
    };

    const createSvgNode = (tagName, attributes = {}, textContent = "") => {
        const node = document.createElementNS(svgNamespace, tagName);
        Object.entries(attributes).forEach(([key, value]) => {
            node.setAttribute(key, String(value));
        });
        if (textContent) {
            node.textContent = textContent;
        }
        return node;
    };

    const formatGraphTickValue = (field, value) => {
        if (!Number.isFinite(value)) {
            return "";
        }

        if (field === "growth") {
            return `${value.toFixed(1)}%`;
        }

        if (field === "score" || field === "ratio") {
            return value.toLocaleString(undefined, {
                minimumFractionDigits: 0,
                maximumFractionDigits: 2,
            });
        }

        if (Math.abs(value) >= 1000) {
            return value.toLocaleString(undefined, {
                notation: "compact",
                maximumFractionDigits: 1,
            });
        }

        return Math.round(value).toLocaleString();
    };

    const formatGraphTimeLabel = (timestamp) => {
        const date = new Date(timestamp);
        const options = graphState.range === "all"
            ? { month: "short", day: "numeric" }
            : { hour: "numeric", minute: "2-digit" };
        return date.toLocaleString(undefined, options);
    };

    const ensureGraphDefs = () => {
        const svg = document.querySelector("#metric-graph");
        if (!svg || svg.querySelector("#graph-area-gradient")) {
            return;
        }

        const defs = createSvgNode("defs");
        const gradient = createSvgNode("linearGradient", {
            id: "graph-area-gradient",
            x1: "0%",
            y1: "0%",
            x2: "0%",
            y2: "100%",
        });
        gradient.appendChild(createSvgNode("stop", {
            offset: "0%",
            "stop-color": "#4d8dff",
            "stop-opacity": "0.2",
        }));
        gradient.appendChild(createSvgNode("stop", {
            offset: "100%",
            "stop-color": "#4d8dff",
            "stop-opacity": "0.02",
        }));
        defs.appendChild(gradient);
        svg.insertBefore(defs, svg.firstChild);
    };

    const formatGraphTooltipTime = (timestamp) => new Date(timestamp).toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
    });

    const showGraphTooltip = (point, metric, entry, delta) => {
        const tooltip = document.querySelector("#graph-hover-tooltip");
        const wrap = document.querySelector(".graph-canvas-wrap");
        const crosshair = document.querySelector("#graph-crosshair");
        if (!tooltip || !wrap || !point) {
            return;
        }

        const deltaLabel = formatDeltaValue(metric, delta) || "No change";
        const deltaClass = delta > 0 ? "is-up" : delta < 0 ? "is-down" : "";

        tooltip.innerHTML = `
            <strong>${metricLabels[metric]}</strong>
            <div class="graph-tooltip-time">${formatGraphTooltipTime(entry.timestamp)}</div>
            <div>Value: ${formatValue(metric, entry.value)}</div>
            <div class="graph-tooltip-delta ${deltaClass}">Change: ${deltaLabel}</div>
        `;

        const pointX = Number(point.getAttribute("cx")) || 0;
        const pointY = Number(point.getAttribute("cy")) || 0;
        const left = Math.min(Math.max(pointX + 14, 12), 900 - 210);
        const top = Math.max(pointY - 80, 12);

        tooltip.style.left = `${left}px`;
        tooltip.style.top = `${top}px`;
        tooltip.classList.add("is-visible");
        point.classList.add("is-hovered");

        if (crosshair) {
            const pointX = Number(point.getAttribute("cx")) || 0;
            const pointY = Number(point.getAttribute("cy")) || 0;
            crosshair.innerHTML = "";
            crosshair.appendChild(createSvgNode("line", {
                x1: pointX,
                x2: pointX,
                y1: 24,
                y2: 272,
            }));
            crosshair.appendChild(createSvgNode("line", {
                x1: 84,
                x2: 870,
                y1: pointY,
                y2: pointY,
            }));
        }
    };

    const hideGraphTooltip = () => {
        const tooltip = document.querySelector("#graph-hover-tooltip");
        const crosshair = document.querySelector("#graph-crosshair");
        if (tooltip) {
            tooltip.classList.remove("is-visible");
        }
        if (crosshair) {
            crosshair.innerHTML = "";
        }
        document.querySelectorAll("#graph-points circle.is-hovered").forEach((point) => {
            point.classList.remove("is-hovered");
        });
    };

    const drawMetricGraph = () => {
        const board = document.querySelector("#metric-graph-board");
        const title = document.querySelector("#graph-title");
        const empty = document.querySelector("#graph-empty");
        const area = document.querySelector("#graph-area");
        const segmentsGroup = document.querySelector("#graph-segments");
        const pointsGroup = document.querySelector("#graph-points");
        const gridGroup = document.querySelector("#graph-grid");
        const axisLabelsGroup = document.querySelector("#graph-axis-labels");

        if (!board || !title || !empty || !area || !segmentsGroup || !pointsGroup || !gridGroup || !axisLabelsGroup) {
            return;
        }

        ensureGraphDefs();
        hideGraphTooltip();

        if (!graphState.metric) {
            title.textContent = "Select a metric graph to view its trend over time.";
            empty.style.display = "grid";
            area.setAttribute("d", "");
            segmentsGroup.innerHTML = "";
            pointsGroup.innerHTML = "";
            gridGroup.innerHTML = "";
            axisLabelsGroup.innerHTML = "";
            hideGraphTooltip();
            return;
        }

        const entries = filterHistoryEntries(getHistoryEntries());
        const values = entries
            .map((entry) => ({
                timestamp: Date.parse(entry.timestamp),
                value: Number(entry[graphState.metric]),
            }))
            .filter((entry) => Number.isFinite(entry.timestamp) && Number.isFinite(entry.value));

        title.textContent = `${metricLabels[graphState.metric]} history`;

        if (values.length < 2) {
            empty.textContent = "Not enough history yet for this metric. Let the dashboard run a bit longer.";
            empty.style.display = "grid";
            area.setAttribute("d", "");
            segmentsGroup.innerHTML = "";
            pointsGroup.innerHTML = "";
            gridGroup.innerHTML = "";
            axisLabelsGroup.innerHTML = "";
            hideGraphTooltip();
            return;
        }

        empty.style.display = "none";
        empty.textContent = "Pick a metric to plot its history.";

        const width = 900;
        const height = 320;
        const padding = { top: 24, right: 30, bottom: 48, left: 84 };
        const minX = Math.min(...values.map((entry) => entry.timestamp));
        const maxX = Math.max(...values.map((entry) => entry.timestamp));
        const minY = Math.min(...values.map((entry) => entry.value));
        const maxY = Math.max(...values.map((entry) => entry.value));
        const xSpan = Math.max(maxX - minX, 1);
        const ySpan = Math.max(maxY - minY, 1);

        const xFor = (value) =>
            padding.left + (((value - minX) / xSpan) * (width - padding.left - padding.right));
        const yFor = (value) =>
            height - padding.bottom - (((value - minY) / ySpan) * (height - padding.top - padding.bottom));

        gridGroup.innerHTML = "";
        axisLabelsGroup.innerHTML = "";
        for (let index = 0; index < 4; index += 1) {
            const y = padding.top + ((height - padding.top - padding.bottom) / 3) * index;
            const lineNode = createSvgNode("line", {
                x1: padding.left,
                x2: width - padding.right,
                y1: y,
                y2: y,
            });
            gridGroup.appendChild(lineNode);

            const tickValue = maxY - ((y - padding.top) / (height - padding.top - padding.bottom)) * ySpan;
            const tickLabel = createSvgNode("text", {
                x: padding.left - 12,
                y: y + 4,
                "text-anchor": "end",
            }, formatGraphTickValue(graphState.metric, tickValue));
            axisLabelsGroup.appendChild(tickLabel);
        }

        for (let index = 0; index < 5; index += 1) {
            const progress = index / 4;
            const x = padding.left + (progress * (width - padding.left - padding.right));
            gridGroup.appendChild(createSvgNode("line", {
                x1: x,
                x2: x,
                y1: padding.top,
                y2: height - padding.bottom,
                class: "graph-grid-vertical",
            }));
        }

        for (let index = 0; index < 5; index += 1) {
            const progress = index / 4;
            const tickTimestamp = minX + (xSpan * progress);
            const x = xFor(tickTimestamp);
            const tickLabel = createSvgNode("text", {
                x,
                y: height - 14,
                "text-anchor": index === 0 ? "start" : index === 4 ? "end" : "middle",
            }, formatGraphTimeLabel(tickTimestamp));
            axisLabelsGroup.appendChild(tickLabel);
        }

        const yAxisTitle = createSvgNode("text", {
            x: 18,
            y: height / 2,
            transform: `rotate(-90 18 ${height / 2})`,
            class: "graph-axis-title",
        }, metricLabels[graphState.metric]);
        axisLabelsGroup.appendChild(yAxisTitle);

        const xAxisTitle = createSvgNode("text", {
            x: width / 2,
            y: height - 2,
            "text-anchor": "middle",
            class: "graph-axis-title",
        }, "Time");
        axisLabelsGroup.appendChild(xAxisTitle);

        const linePath = values
            .map((entry, index) => `${index === 0 ? "M" : "L"} ${xFor(entry.timestamp)} ${yFor(entry.value)}`)
            .join(" ");
        const areaPath = `${linePath} L ${xFor(values[values.length - 1].timestamp)} ${height - padding.bottom} L ${xFor(values[0].timestamp)} ${height - padding.bottom} Z`;
        area.setAttribute("d", areaPath);

        segmentsGroup.innerHTML = "";
        values.slice(0, -1).forEach((entry, index) => {
            const nextEntry = values[index + 1];
            const segment = createSvgNode("line", {
                x1: xFor(entry.timestamp),
                y1: yFor(entry.value),
                x2: xFor(nextEntry.timestamp),
                y2: yFor(nextEntry.value),
                class: nextEntry.value >= entry.value ? "is-up" : "is-down",
            });
            segmentsGroup.appendChild(segment);
        });

        pointsGroup.innerHTML = "";
        values.forEach((entry, index) => {
            const previousValue = values[Math.max(index - 1, 0)]?.value ?? entry.value;
            const delta = entry.value - previousValue;
            const pointClass = entry.value > previousValue
                ? "is-up"
                : entry.value < previousValue
                    ? "is-down"
                    : "is-flat";
            const point = createSvgNode("circle", {
                cx: xFor(entry.timestamp),
                cy: yFor(entry.value),
                r: 4,
                class: pointClass,
            });
            point.dataset.delta = String(delta);
            point.addEventListener("mouseenter", () => {
                showGraphTooltip(point, graphState.metric, entry, delta);
            });
            point.addEventListener("mouseleave", hideGraphTooltip);
            pointsGroup.appendChild(point);
        });
    };

    const syncGraphStateFromControls = () => {
        const metricSelect = document.querySelector("#graph-metric-select");
        const rangeSelect = document.querySelector("#graph-range-select");
        const startInput = document.querySelector("#graph-start");
        const endInput = document.querySelector("#graph-end");
        const customRange = document.querySelector("#graph-custom-range");
        const historyMeta = document.querySelector("#graph-history-meta");
        const bounds = getHistoryBounds();
        const summary = getHistorySummary();

        if (metricSelect) {
            metricSelect.value = graphState.metric || "";
        }
        if (rangeSelect) {
            rangeSelect.value = graphState.range || "live";
        }
        if (startInput) {
            startInput.value = graphState.customStart || "";
            startInput.min = toDatetimeLocalValue(bounds.min);
            startInput.max = toDatetimeLocalValue(bounds.max);
        }
        if (endInput) {
            endInput.value = graphState.customEnd || "";
            endInput.min = toDatetimeLocalValue(bounds.min);
            endInput.max = toDatetimeLocalValue(bounds.max);
        }
        if (customRange) {
            customRange.classList.toggle("is-hidden", graphState.range !== "custom");
        }
        if (historyMeta) {
            const startText = summary.start ? formatGraphTooltipTime(summary.start) : "No history yet";
            const endText = summary.end ? formatGraphTooltipTime(summary.end) : "No history yet";
            historyMeta.innerHTML = `
                <strong>${summary.count || 0}</strong>
                <span>snapshots available from ${startText} to ${endText}</span>
            `;
        }
    };

    const setupMetricGraphs = () => {
        const board = document.querySelector("#metric-graph-board");
        if (!board) {
            return;
        }

        const metricSelect = document.querySelector("#graph-metric-select");
        const rangeSelect = document.querySelector("#graph-range-select");

        if (metricSelect) {
            metricSelect.onchange = () => {
                graphState.metric = metricSelect.value || null;
                syncGraphStateFromControls();
                drawMetricGraph();
            };
        }

        if (rangeSelect) {
            rangeSelect.onchange = () => {
                graphState.range = rangeSelect.value;
                const bounds = getHistoryBounds();
                if (graphState.range !== "custom") {
                    graphState.customStart = "";
                    graphState.customEnd = "";
                } else if (!graphState.customStart && !graphState.customEnd) {
                    graphState.customStart = toDatetimeLocalValue(bounds.min);
                    graphState.customEnd = toDatetimeLocalValue(bounds.max);
                }
                syncGraphStateFromControls();
                drawMetricGraph();
            };
        }

        const applyRange = document.querySelector("#apply-graph-range");
        const startInput = document.querySelector("#graph-start");
        const endInput = document.querySelector("#graph-end");

        if (applyRange && startInput && endInput) {
            applyRange.onclick = () => {
                graphState.range = "custom";
                const startValue = startInput.value;
                const endValue = endInput.value;

                if (startValue && endValue && Date.parse(startValue) > Date.parse(endValue)) {
                    graphState.customStart = endValue;
                    graphState.customEnd = startValue;
                } else {
                    graphState.customStart = startValue;
                    graphState.customEnd = endValue;
                }

                syncGraphStateFromControls();
                drawMetricGraph();
            };
        }

        syncGraphStateFromControls();
        drawMetricGraph();
    };

    const applyOpportunityClass = (element, nextClass) => {
        if (!element) {
            return;
        }

        opportunityClasses.forEach((className) => element.classList.remove(className));
        if (nextClass) {
            element.classList.add(nextClass);
            element.dataset.opportunityClass = nextClass;
        }
    };

    const getOpportunityLabelText = (element) => {
        if (!element) {
            return "";
        }

        const textNode = Array.from(element.childNodes || []).find(
            (node) => node.nodeType === Node.TEXT_NODE && node.textContent.trim(),
        );

        return textNode ? textNode.textContent.trim() : (element.textContent || "").trim();
    };

    const setOpportunityLabelText = (element, nextText) => {
        if (!element) {
            return;
        }

        const textNode = Array.from(element.childNodes || []).find(
            (node) => node.nodeType === Node.TEXT_NODE,
        );

        if (textNode) {
            textNode.textContent = ` ${nextText} `;
        } else {
            element.insertBefore(document.createTextNode(` ${nextText} `), element.firstChild);
        }
    };

    const syncOpportunityElement = (element, nextText, nextClass) => {
        if (!element) {
            return;
        }

        const currentClass = element.dataset.opportunityClass || "";
        const currentText = getOpportunityLabelText(element);

        if (currentClass !== nextClass || currentText !== nextText) {
            setOpportunityLabelText(element, nextText);
            applyOpportunityClass(element, nextClass);
            pulseOpportunityElement(element);
        }
    };

    const syncTopStreamersBoard = (currentBoard, nextBoard) => {
        if (!currentBoard || !nextBoard) {
            return;
        }

        const currentGrid = currentBoard.querySelector(".top-streamers-grid");
        const nextGrid = nextBoard.querySelector(".top-streamers-grid");
        const currentCards = [...currentBoard.querySelectorAll(".streamer-card")];
        const nextCards = [...nextBoard.querySelectorAll(".streamer-card")];

        if (!currentGrid || !nextGrid || currentCards.length !== nextCards.length) {
            currentBoard.innerHTML = nextBoard.innerHTML;
            return;
        }

        const currentByKey = new Map(
            currentCards.map((card) => [card.dataset.streamerKey || "", card]),
        );

        const orderedCards = nextCards.map((nextCard) => {
            const key = nextCard.dataset.streamerKey || "";
            const currentCard = currentByKey.get(key);

            if (!currentCard) {
                return nextCard;
            }

            const currentName = currentCard.querySelector(".streamer-copy strong");
            const nextName = nextCard.querySelector(".streamer-copy strong");
            if (currentName && nextName) {
                currentName.textContent = nextName.textContent;
            }

            const currentRank = currentCard.querySelector(".streamer-rank");
            const nextRank = nextCard.querySelector(".streamer-rank");
            if (currentRank && nextRank) {
                currentRank.textContent = nextRank.textContent;
            }

            currentCard.href = nextCard.href;
            syncImage(
                currentCard.querySelector(".streamer-media"),
                nextCard.querySelector(".streamer-media"),
            );

            const currentViewers = currentCard.querySelector("[data-top-streamer-viewers]");
            const nextViewers = nextCard.querySelector("[data-top-streamer-viewers]");
            if (currentViewers && nextViewers) {
                animateNumericElement(currentViewers, "viewers", parseValue(nextViewers));
            }

            return currentCard;
        });

        currentGrid.replaceChildren(...orderedCards);
    };

    const syncImage = (currentContainer, nextContainer) => {
        if (!currentContainer || !nextContainer) {
            return;
        }

        const currentImage = currentContainer.querySelector("img");
        const nextImage = nextContainer.querySelector("img");

        if (currentImage && nextImage) {
            currentImage.src = nextImage.src;
            currentImage.alt = nextImage.alt;
            return;
        }

        if (!currentImage && !nextImage) {
            currentContainer.innerHTML = nextContainer.innerHTML;
            return;
        }

        currentContainer.innerHTML = nextContainer.innerHTML;
    };

    const updateGalleryCard = (currentCard, nextCard) => {
        currentCard.href = nextCard.href;
        currentCard.dataset.categoryName = nextCard.dataset.categoryName || "";
        currentCard.dataset.gameKey = nextCard.dataset.gameKey || "";

        const currentRank = currentCard.querySelector(".gallery-rank");
        const nextRank = nextCard.querySelector(".gallery-rank");
        if (currentRank && nextRank) {
            currentRank.textContent = nextRank.textContent;
        }

        const currentTitle = currentCard.querySelector(".gallery-copy strong");
        const nextTitle = nextCard.querySelector(".gallery-copy strong");
        if (currentTitle && nextTitle) {
            currentTitle.textContent = nextTitle.textContent;
        }

        syncImage(
            currentCard.querySelector(".gallery-media"),
            nextCard.querySelector(".gallery-media"),
        );

        const currentScore = currentCard.querySelector('[data-live-field="score"]');
        const nextScore = nextCard.querySelector('[data-live-field="score"]');
        if (currentScore && nextScore) {
            animateNumericElement(currentScore, "score", parseValue(nextScore));
        }

        const currentViewers = currentCard.querySelector('[data-live-field="viewers"]');
        const nextViewers = nextCard.querySelector('[data-live-field="viewers"]');
        if (currentViewers && nextViewers) {
            animateNumericElement(currentViewers, "viewers", parseValue(nextViewers));
        }

        const currentOpportunity = currentCard.querySelector('[data-live-field="opportunity"]');
        const nextOpportunity = nextCard.querySelector('[data-live-field="opportunity"]');
        if (currentOpportunity && nextOpportunity) {
            syncOpportunityElement(
                currentOpportunity,
                getOpportunityLabelText(nextOpportunity),
                nextOpportunity.dataset.opportunityClass || "",
            );
        }
    };

    const updateTrackedCount = (nextDocument) => {
        const currentCount = document.querySelector(".chooser-meta strong");
        const nextCount = nextDocument.querySelector(".chooser-meta strong");
        if (!currentCount || !nextCount) {
            return;
        }

        const nextValue = parseValue(nextCount, Number(nextCount.textContent.replace(/,/g, "")));
        animateNumericElement(currentCount, "tracked_count", nextValue);
    };

    const updateBreakoutCard = (currentCard, nextCard) => {
        currentCard.href = nextCard.href;
        currentCard.dataset.gameKey = nextCard.dataset.gameKey || "";

        const currentTitle = currentCard.querySelector(".breakout-top strong");
        const nextTitle = nextCard.querySelector(".breakout-top strong");
        if (currentTitle && nextTitle) {
            currentTitle.textContent = nextTitle.textContent;
        }

        const currentSignal = currentCard.querySelector(".breakout-signal");
        const nextSignal = nextCard.querySelector(".breakout-signal");
        if (currentSignal && nextSignal) {
            currentSignal.textContent = nextSignal.textContent;
        }

        syncImage(
            currentCard.querySelector(".breakout-media"),
            nextCard.querySelector(".breakout-media"),
        );

        const currentScore = currentCard.querySelector('[data-live-field="score"]');
        const nextScore = nextCard.querySelector('[data-live-field="score"]');
        if (currentScore && nextScore) {
            animateNumericElement(currentScore, "score", parseValue(nextScore));
        }

        const currentViewers = currentCard.querySelector('[data-live-field="viewers"]');
        const nextViewers = nextCard.querySelector('[data-live-field="viewers"]');
        if (currentViewers && nextViewers) {
            animateNumericElement(currentViewers, "viewers", parseValue(nextViewers));
        }

        const currentOpportunity = currentCard.querySelector('[data-live-field="opportunity"]');
        const nextOpportunity = nextCard.querySelector('[data-live-field="opportunity"]');
        if (currentOpportunity && nextOpportunity) {
            syncOpportunityElement(
                currentOpportunity,
                getOpportunityLabelText(nextOpportunity),
                nextOpportunity.dataset.opportunityClass || "",
            );
        }
    };

    const syncBreakoutBoard = (nextDocument) => {
        const currentBoard = document.querySelector("#breakout-board");
        const nextBoard = nextDocument.querySelector("#breakout-board");

        if (!currentBoard && !nextBoard) {
            return true;
        }

        if (!currentBoard && nextBoard) {
            const chooserBoard = document.querySelector(".chooser-board");
            if (chooserBoard) {
                chooserBoard.insertAdjacentElement("beforebegin", nextBoard);
                initializeBaselineTrendColors();
            }
            return true;
        }

        if (currentBoard && !nextBoard) {
            currentBoard.remove();
            return true;
        }

        if (!currentBoard || !nextBoard) {
            return false;
        }

        const currentCards = new Map(
            [...currentBoard.querySelectorAll(".breakout-card")].map((card) => [
                card.dataset.gameKey || card.href,
                card,
            ]),
        );

        const orderedCards = [];
        [...nextBoard.querySelectorAll(".breakout-card")].forEach((nextCard) => {
            const key = nextCard.dataset.gameKey || nextCard.href;
            const currentCard = currentCards.get(key);

            if (currentCard) {
                updateBreakoutCard(currentCard, nextCard);
                orderedCards.push(currentCard);
                currentCards.delete(key);
            } else {
                orderedCards.push(nextCard);
            }
        });

        const currentGrid = currentBoard.querySelector(".breakout-grid");
        if (currentGrid) {
            currentGrid.replaceChildren(...orderedCards);
        }

        const currentDescription = currentBoard.querySelector(".breakout-description");
        const nextDescription = nextBoard.querySelector(".breakout-description");
        if (currentDescription && nextDescription) {
            currentDescription.textContent = nextDescription.textContent;
        }

        initializeBaselineTrendColors();
        return true;
    };

    const syncChooserBoard = (nextDocument) => {
        const currentGallery = document.querySelector("#category-list");
        const nextGallery = nextDocument.querySelector("#category-list");

        if (!currentGallery || !nextGallery) {
            return false;
        }

        const scrollTop = currentGallery.scrollTop;
        const currentCards = new Map(
            [...currentGallery.querySelectorAll(".gallery-card")].map((card) => [
                card.dataset.gameKey || card.href,
                card,
            ]),
        );

        const orderedCards = [];
        [...nextGallery.querySelectorAll(".gallery-card")].forEach((nextCard) => {
            const key = nextCard.dataset.gameKey || nextCard.href;
            const currentCard = currentCards.get(key);

            if (currentCard) {
                updateGalleryCard(currentCard, nextCard);
                orderedCards.push(currentCard);
                currentCards.delete(key);
            } else {
                orderedCards.push(nextCard);
            }
        });

        currentGallery.replaceChildren(...orderedCards);
        currentGallery.scrollTop = scrollTop;
        updateTrackedCount(nextDocument);
        syncBreakoutBoard(nextDocument);
        initializeBaselineTrendColors();
        setupCategorySearch();
        return true;
    };

    const syncSelectedBoard = (nextDocument) => {
        const currentBoard = document.querySelector(".selected-board");
        const nextBoard = nextDocument.querySelector(".selected-board");

        if (!currentBoard || !nextBoard) {
            return false;
        }

        const currentTitle = currentBoard.querySelector("[data-selected-title]");
        const nextTitle = nextBoard.querySelector("[data-selected-title]");
        if (currentTitle && nextTitle) {
            currentTitle.textContent = nextTitle.textContent;
        }

        const currentDescription = currentBoard.querySelector(".selected-subtext");
        const nextDescription = nextBoard.querySelector(".selected-subtext");
        if (currentDescription && nextDescription) {
            currentDescription.textContent = nextDescription.textContent;
        }

        const currentHistory = document.querySelector("#history-data");
        const nextHistory = nextDocument.querySelector("#history-data");
        if (currentHistory && nextHistory) {
            currentHistory.textContent = nextHistory.textContent;
        }

        const currentHistorySummary = document.querySelector("#history-summary-data");
        const nextHistorySummary = nextDocument.querySelector("#history-summary-data");
        if (currentHistorySummary && nextHistorySummary) {
            currentHistorySummary.textContent = nextHistorySummary.textContent;
        }

        const currentAnalyticsData = document.querySelector("#selected-analytics-data");
        const nextAnalyticsData = nextDocument.querySelector("#selected-analytics-data");
        if (currentAnalyticsData && nextAnalyticsData) {
            currentAnalyticsData.textContent = nextAnalyticsData.textContent;
        }

        syncImage(
            currentBoard.querySelector(".selected-image"),
            nextBoard.querySelector(".selected-image"),
        );

        const fields = ["score", "viewers", "streams", "ratio", "growth", "viewer_change"];
        fields.forEach((field) => {
            const currentElements = [...currentBoard.querySelectorAll(`[data-live-field="${field}"]`)];
            const nextElements = [...nextBoard.querySelectorAll(`[data-live-field="${field}"]`)];
            currentElements.forEach((element, index) => {
                const nextElement = nextElements[index];
                if (nextElement) {
                    const currentValue = parseVisibleValue(
                        element,
                        field,
                        parseValue(element, parseValue(nextElement)),
                    );
                    const nextValue = parseValue(nextElement);
                    animateNumericElement(element, field, parseValue(nextElement));
                    syncDeltaIndicator(currentBoard, field, nextValue - currentValue);
                }
            });
        });

        const currentOpportunity = currentBoard.querySelectorAll('[data-live-field="opportunity"]');
        const nextOpportunity = nextBoard.querySelectorAll('[data-live-field="opportunity"]');
        currentOpportunity.forEach((element, index) => {
            const nextElement = nextOpportunity[index];
            if (nextElement) {
                syncOpportunityElement(
                    element,
                    getOpportunityLabelText(nextElement),
                    nextElement.dataset.opportunityClass || "",
                );
            }
        });

        const currentAnalyticsBoard = currentBoard.querySelector("#selected-analytics");
        const nextAnalyticsBoard = nextBoard.querySelector("#selected-analytics");
        if (currentAnalyticsBoard && nextAnalyticsBoard) {
            currentAnalyticsBoard.innerHTML = nextAnalyticsBoard.innerHTML;
        }

        const syncSection = (selector) => {
            const currentSection = currentBoard.querySelector(selector);
            const nextSection = nextBoard.querySelector(selector);

            if (currentSection && nextSection) {
                currentSection.replaceWith(nextSection);
                return;
            }

            if (!currentSection && nextSection) {
                const graphBoard = currentBoard.querySelector(".graph-board");
                if (graphBoard) {
                    graphBoard.insertAdjacentElement("beforebegin", nextSection);
                } else {
                    currentBoard.appendChild(nextSection);
                }
                return;
            }

            if (currentSection && !nextSection) {
                currentSection.remove();
            }
        };

        syncSection(".streaming-outlook");
        syncSection(".similar-categories");
        syncSection(".also-watch-section");

        const currentTopStreamers = document.querySelector(".top-streamers-board");
        const nextTopStreamers = nextDocument.querySelector(".top-streamers-board");
        if (currentTopStreamers && nextTopStreamers) {
            syncTopStreamersBoard(currentTopStreamers, nextTopStreamers);
        } else if (!currentTopStreamers && nextTopStreamers) {
            const statsGrid = currentBoard.querySelector(".stats-grid");
            if (statsGrid) {
                statsGrid.insertAdjacentElement("beforebegin", nextTopStreamers);
            }
        } else if (currentTopStreamers && !nextTopStreamers) {
            currentTopStreamers.remove();
        }

        drawMetricGraph();

        return true;
    };

    const isSearchActive = () => {
        const searchInput = document.querySelector("#category-search");
        const suggestionsBox = document.querySelector("#search-suggestions");

        return Boolean(
            searchInput &&
            (
                document.activeElement === searchInput ||
                searchInput.value.trim() !== "" ||
                suggestionsBox?.classList.contains("is-open")
            )
        );
    };

    const setupCategorySearch = () => {
        const searchInput = document.querySelector("#category-search");
        const suggestionsBox = document.querySelector("#search-suggestions");

        if (!searchInput || !suggestionsBox) {
            return;
        }

        const getCategoryItems = () => [...document.querySelectorAll("[data-category-link]")].map((link) => ({
            name: (link.getAttribute("data-category-name") || "").trim(),
            label: link.querySelector(".gallery-copy strong")?.textContent?.trim() || "",
            href: link.getAttribute("href"),
            element: link,
        }));

        let activeSuggestionIndex = -1;

        const renderSuggestions = (matches) => {
            suggestionsBox.innerHTML = "";
            activeSuggestionIndex = -1;

            if (matches.length === 0 || searchInput.value.trim() === "") {
                suggestionsBox.classList.remove("is-open");
                return;
            }

            matches.slice(0, 6).forEach((item) => {
                const button = document.createElement("button");
                button.type = "button";
                button.className = "suggestion-item";
                button.textContent = item.label;
                button.dataset.href = item.href;
                button.onclick = () => {
                    window.location.href = item.href;
                };
                suggestionsBox.appendChild(button);
            });

            suggestionsBox.classList.add("is-open");
        };

        const filterCategories = () => {
            const query = searchInput.value.trim().toLowerCase();
            const categoryItems = getCategoryItems();

            categoryItems.forEach((item) => {
                item.element.style.display = !query || item.name.includes(query) ? "" : "none";
            });

            renderSuggestions(categoryItems.filter((item) => item.name.includes(query)));
        };

        searchInput.oninput = filterCategories;
        searchInput.onfocus = filterCategories;
        searchInput.onkeydown = (event) => {
            const suggestionButtons = [...suggestionsBox.querySelectorAll(".suggestion-item")];
            if (suggestionButtons.length === 0) {
                return;
            }

            if (event.key === "ArrowDown") {
                event.preventDefault();
                activeSuggestionIndex = Math.min(activeSuggestionIndex + 1, suggestionButtons.length - 1);
            } else if (event.key === "ArrowUp") {
                event.preventDefault();
                activeSuggestionIndex = Math.max(activeSuggestionIndex - 1, 0);
            } else if (event.key === "Enter" && activeSuggestionIndex >= 0) {
                event.preventDefault();
                suggestionButtons[activeSuggestionIndex].click();
                return;
            } else if (event.key === "Escape") {
                suggestionsBox.classList.remove("is-open");
                return;
            } else {
                return;
            }

            suggestionButtons.forEach((button, index) => {
                button.classList.toggle("is-active", index === activeSuggestionIndex);
            });
        };

        if (!document.body.dataset.searchBound) {
            document.addEventListener("click", (event) => {
                const activeInput = document.querySelector("#category-search");
                const activeSuggestions = document.querySelector("#search-suggestions");

                if (
                    activeSuggestions &&
                    activeInput &&
                    !activeSuggestions.contains(event.target) &&
                    event.target !== activeInput
                ) {
                    activeSuggestions.classList.remove("is-open");
                }
            });
            document.body.dataset.searchBound = "true";
        }
    };

    const refreshPageContent = async () => {
        if (isSearchActive()) {
            return;
        }

        try {
            const response = await fetch(window.location.href, {
                cache: "no-store",
                headers: {
                    "X-Requested-With": "fetch",
                },
            });

            if (!response.ok) {
                return;
            }

            const html = await response.text();
            const nextDocument = new DOMParser().parseFromString(html, "text/html");
            const nextTitle = nextDocument.querySelector("title");

            if (nextTitle) {
                document.title = nextTitle.textContent;
            }

            const updated =
                syncChooserBoard(nextDocument) ||
                syncSelectedBoard(nextDocument);

            if (!updated) {
                const nextShell = nextDocument.querySelector(".simple-shell");
                const currentShell = document.querySelector(".simple-shell");

                if (nextShell && currentShell) {
                    currentShell.innerHTML = nextShell.innerHTML;
                    setupCategorySearch();
                    setupMetricGraphs();
                    initializeBaselineTrendColors();
                }
            }
        } catch (error) {
            console.debug("Background refresh skipped", error);
        }
    };

    requestAnimationFrame(() => {
        document.body.classList.remove("preload");
        document.body.classList.add("is-ready");
    });

    setupCategorySearch();
    setupMetricGraphs();
    initializeBaselineTrendColors();
    window.setInterval(refreshPageContent, refreshIntervalMs);
});
