import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { forceCollide } from "d3-force";
import ForceGraph2D, { type ForceGraphMethods } from "react-force-graph-2d";
import { useAppState, useAppDispatch } from "../../context/hooks";
import { useGraph } from "../../hooks/useGraph";
import type { GraphNode, GraphLink } from "../../types";

import { ActorDetail } from "../ActorDetail/ActorDetail";
import { MovieDetail } from "../MovieDetail/MovieDetail";
import { profileUrl } from "../../utils/images";

// --- Layout & styling constants (adjust these to tune spacing) ---
const CENTER_RADIUS = 24;
const NODE_RADIUS = 16;
const CHARGE_STRENGTH = -600;
const ACCENT = "#6366f1";

// const NODE_COLORS = [
//   "#e05c5c", // red
//   "#e0905c", // orange
//   "#c8c84a", // yellow
//   "#4caf7d", // green
//   "#5c8fe0", // blue
//   "#9b5ce0", // purple
// ];

// function nodeColor(node: GraphNode): string {
//   return NODE_COLORS[node.id % NODE_COLORS.length];
// }

function nodeRadius(node: GraphNode) {
  return node.is_center ? CENTER_RADIUS : NODE_RADIUS;
}

/** Draw an image cropped to fill a circle (like CSS object-fit: cover). */
function drawCoverImage(
  ctx: CanvasRenderingContext2D,
  img: HTMLImageElement,
  cx: number,
  cy: number,
  r: number,
) {
  const { naturalWidth: sw, naturalHeight: sh } = img;
  const diameter = r * 2;
  // Scale so the shorter side fills the diameter
  const scale = Math.max(diameter / sw, diameter / sh);
  const dw = sw * scale;
  const dh = sh * scale;
  // Center the image over the circle
  ctx.drawImage(img, cx - dw / 2, cy - dh / 2, dw, dh);
}

export function GraphView() {
  const { selectedActor, detailPanel } = useAppState();
  const dispatch = useAppDispatch();
  const { graph, loading, error, loadNeighborhood } = useGraph();
  const graphRef = useRef<ForceGraphMethods<GraphNode>>(undefined);
  const imageCache = useRef(new Map<number, HTMLImageElement>());
  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 });

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      setDimensions({ width, height });
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    const fg = graphRef.current;
    if (!fg || !graph) return;
    // Distance varies inversely with shared-movie weight so heavily-connected
    // actors pull closer while weak connections stay further out.
    fg.d3Force("link")?.distance(
      (link: GraphLink) => 80 + 150 / Math.sqrt(link.weight || 1),
    );
    fg.d3Force("charge")?.strength(CHARGE_STRENGTH);
    // Collision: 3 iterations per tick resolves crowded layouts that a single
    // pass (the default) leaves unfinished within the cooldown budget.
    fg.d3Force("collision", forceCollide(NODE_RADIUS + 16).iterations(3));
    fg.d3ReheatSimulation();
  }, [graph]);

  useEffect(() => {
    if (selectedActor) {
      loadNeighborhood(selectedActor.tmdb_id);
    }
  }, [selectedActor, loadNeighborhood]);

  // Pre-load image for the center node only
  useEffect(() => {
    if (!graph) return;
    let active = true;
    const center = graph.nodes.find((n) => n.is_center);
    if (center && center.profile_path && !imageCache.current.has(center.id)) {
      const img = new Image();
      img.crossOrigin = "anonymous";
      img.src = profileUrl(center.profile_path, "w185")!;
      img.onload = () => {
        if (!active) return;
        imageCache.current.set(center.id, img);
        (graphRef.current as unknown as { refresh(): void })?.refresh();
      };
      imageCache.current.set(center.id, img);
    }
    return () => {
      active = false;
    };
  }, [graph]);

  const nodeCanvasObject = useCallback(
    (node: GraphNode, ctx: CanvasRenderingContext2D, globalScale: number) => {
      const x = (node as GraphNode & { x: number }).x;
      const y = (node as GraphNode & { y: number }).y;
      if (x == null || y == null) return;

      const r = nodeRadius(node);
      const img = imageCache.current.get(node.id);
      const loaded = img?.complete && img.naturalWidth > 0;

      ctx.save();

      // Highlight ring for center
      if (node.is_center) {
        ctx.beginPath();
        ctx.arc(x, y, r + 3, 0, 2 * Math.PI);
        ctx.fillStyle = ACCENT;
        ctx.fill();
      }

      // Clip circle
      ctx.beginPath();
      ctx.arc(x, y, r, 0, 2 * Math.PI);
      ctx.closePath();
      ctx.clip();

      if (loaded) {
        drawCoverImage(ctx, img!, x, y, r);
      } else {
        ctx.strokeStyle = "white";
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.arc(x, y, r + 10, 0, 2 * Math.PI);
        ctx.fillStyle = ACCENT;
        ctx.fill();
        ctx.closePath();

        // ctx.fillStyle = nodeColor(node);
        // ctx.fill();
      }

      ctx.restore();

      // Name label (isolated save/restore to prevent shadow state leak)
      ctx.save();
      const fontSize = Math.max(10, 12 / globalScale);
      ctx.font = `${fontSize}px sans-serif`;
      ctx.textAlign = "center";
      ctx.textBaseline = "bottom";
      ctx.shadowColor = "rgba(0,0,0,0.8)";
      ctx.shadowBlur = 3;
      ctx.fillStyle = "#e8e8ed";
      ctx.fillText(node.name, x, y + r + 4);
      ctx.restore();
    },
    [],
  );

  const linkCanvasObject = useCallback(
    (link: GraphLink, ctx: CanvasRenderingContext2D) => {
      const src = link.source as unknown as { x: number; y: number };
      const tgt = link.target as unknown as { x: number; y: number };
      if (src?.x == null || tgt?.x == null) return;

      ctx.beginPath();
      ctx.moveTo(src.x, src.y);
      ctx.lineTo(tgt.x, tgt.y);
      ctx.strokeStyle = "rgba(255,255,255,0.15)";
      ctx.lineWidth = 1 + link.weight * 0.5;
      ctx.stroke();
    },
    [],
  );

  const nodeLabel = useCallback((node: GraphNode) => {
    return `${node.name}`;
  }, []);

  const handleNodeClick = useCallback(
    (node: GraphNode) => {
      if (node.is_center) {
        dispatch({
          type: "OPEN_DETAIL",
          detail: { type: "actor", tmdb_id: node.id },
        });
      } else {
        dispatch({
          type: "SELECT_ACTOR",
          actor: { tmdb_id: node.id, name: node.name },
        });
      }
    },
    [dispatch],
  );

  const handleNodeRightClick = useCallback(
    (node: GraphNode, event: MouseEvent) => {
      event.preventDefault();
      dispatch({
        type: "SELECT_SECOND_ACTOR",
        actor: { tmdb_id: node.id, name: node.name },
      });
    },
    [dispatch],
  );

  const graphData = useMemo(
    () =>
      graph
        ? { nodes: graph.nodes, links: graph.links }
        : { nodes: [], links: [] },
    [graph],
  );

  return (
    <div className="flex w-full h-full">
      <div className="relative flex-1 min-w-0" ref={containerRef}>
        {loading && (
          <div className="absolute top-3 left-1/2 -translate-x-1/2 px-4 py-1.5 bg-surface border border-border rounded-lg text-[13px] text-muted-foreground z-[5]">
            Loading graph...
          </div>
        )}
        {error && (
          <div className="absolute top-3 left-1/2 -translate-x-1/2 px-4 py-1.5 bg-red-500/15 border border-red-500/30 rounded-lg text-[13px] text-red-300 z-[5]">
            {error}
          </div>
        )}
        <ForceGraph2D
          ref={graphRef}
          graphData={graphData}
          nodeId="id"
          nodeCanvasObject={nodeCanvasObject}
          nodePointerAreaPaint={(node, color, ctx) => {
            const n = node as GraphNode & { x: number; y: number };
            if (n.x == null || n.y == null) return;
            const r = nodeRadius(n);
            ctx.fillStyle = color;
            ctx.beginPath();
            ctx.arc(n.x, n.y, r, 0, 2 * Math.PI);
            ctx.fill();
          }}
          linkCanvasObject={linkCanvasObject}
          nodeLabel={nodeLabel}
          onNodeClick={handleNodeClick}
          onNodeRightClick={handleNodeRightClick}
          backgroundColor="rgba(0,0,0,0)"
          cooldownTicks={400}
          width={dimensions.width}
          height={dimensions.height}
        />
      </div>
      {detailPanel && (
        <aside className="w-85 shrink-0 h-full bg-surface border-l border-border overflow-y-auto animate-slide-in">
          {detailPanel.type === "actor" ? (
            <ActorDetail tmdbId={detailPanel.tmdb_id} />
          ) : (
            <MovieDetail tmdbId={detailPanel.tmdb_id} />
          )}
        </aside>
      )}
    </div>
  );
}
