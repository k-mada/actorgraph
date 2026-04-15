import { useState, useCallback } from 'react';
import type { PathResult } from '../types';
import { getShortestPath } from '../services/api';

export function useShortestPath() {
  const [result, setResult] = useState<PathResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const findPath = useCallback(async (actor1Id: number, actor2Id: number, maxDepth?: number) => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const data = await getShortestPath(actor1Id, actor2Id, maxDepth);
      setResult(data);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, []);

  const clear = useCallback(() => {
    setResult(null);
    setError(null);
  }, []);

  return { result, loading, error, findPath, clear };
}
