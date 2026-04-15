import { useState, useCallback } from 'react';
import type { GraphData, GraphNode, GraphLink } from '../types';
import { getActorNeighborhood, expandGraphNode } from '../services/api';

function linkId(link: GraphLink): string {
  const s = typeof link.source === 'object' ? (link.source as GraphNode).id : link.source;
  const t = typeof link.target === 'object' ? (link.target as GraphNode).id : link.target;
  return `${s}-${t}`;
}

function mergeGraphs(existing: GraphData, incoming: GraphData): GraphData {
  const nodeMap = new Map<number, GraphNode>();
  // Preserve existing nodes (they carry x/y from simulation)
  for (const n of existing.nodes) nodeMap.set(n.id, n);
  for (const n of incoming.nodes) {
    if (!nodeMap.has(n.id)) nodeMap.set(n.id, n);
  }

  const linkSet = new Set<string>();
  const links: GraphLink[] = [];
  for (const l of existing.links) {
    const id = linkId(l);
    if (!linkSet.has(id)) {
      linkSet.add(id);
      links.push(l);
    }
  }
  for (const l of incoming.links) {
    const id = linkId(l);
    if (!linkSet.has(id)) {
      linkSet.add(id);
      links.push(l);
    }
  }

  return {
    nodes: Array.from(nodeMap.values()),
    links,
    center_tmdb_id: existing.center_tmdb_id,
  };
}

export function useGraph() {
  const [graph, setGraph] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadNeighborhood = useCallback(async (tmdbId: number) => {
    setLoading(true);
    setError(null);
    try {
      const data = await getActorNeighborhood(tmdbId);
      setGraph(data);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, []);

  const expandNode = useCallback(async (tmdbId: number) => {
    setLoading(true);
    setError(null);
    try {
      const data = await expandGraphNode(tmdbId);
      setGraph((prev) => (prev ? mergeGraphs(prev, data) : data));
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, []);

  return { graph, loading, error, loadNeighborhood, expandNode };
}
