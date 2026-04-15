import type {
  SearchResults,
  Actor,
  Movie,
  GraphData,
  PathResult,
  CommonMoviesResult,
  FilmographyResult,
} from "../types";

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "";
const API_KEY = import.meta.env.VITE_API_KEY ?? "";

async function fetchApi<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { "X-API-Key": API_KEY },
    signal,
  });
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${res.statusText}`);
  }
  return res.json();
}

export function searchAll(
  query: string,
  signal?: AbortSignal,
): Promise<SearchResults> {
  return fetchApi(`/api/search?q=${encodeURIComponent(query)}`, signal);
}

export function getActor(tmdbId: number, signal?: AbortSignal): Promise<Actor> {
  return fetchApi(`/api/actors/${tmdbId}`, signal);
}

export function getMovie(tmdbId: number, signal?: AbortSignal): Promise<Movie> {
  return fetchApi(`/api/movies/${tmdbId}`, signal);
}

export function getActorNeighborhood(
  tmdbId: number,
  opts?: { limit?: number; year_from?: number; year_to?: number },
  signal?: AbortSignal,
): Promise<GraphData> {
  const params = new URLSearchParams();
  if (opts?.limit) params.set("limit", String(opts.limit));
  if (opts?.year_from) params.set("year_from", String(opts.year_from));
  if (opts?.year_to) params.set("year_to", String(opts.year_to));
  const qs = params.toString();
  return fetchApi(
    `/api/graph/neighborhood/${tmdbId}${qs ? `?${qs}` : ""}`,
    signal,
  );
}

export function expandGraphNode(
  tmdbId: number,
  signal?: AbortSignal,
): Promise<GraphData> {
  return fetchApi(`/api/graph/expand/${tmdbId}`, signal);
}

export function getShortestPath(
  actor1Id: number,
  actor2Id: number,
  maxDepth?: number,
  signal?: AbortSignal,
): Promise<PathResult> {
  const qs = maxDepth ? `?max_depth=${maxDepth}` : "";
  return fetchApi(`/api/actors/${actor1Id}/path/${actor2Id}${qs}`, signal);
}

export function getCommonMovies(
  actor1Id: number,
  actor2Id: number,
  signal?: AbortSignal,
): Promise<CommonMoviesResult> {
  return fetchApi(`/api/actors/${actor1Id}/common-movies/${actor2Id}`, signal);
}

export function getActorFilmography(
  tmdbId: number,
  signal?: AbortSignal,
): Promise<FilmographyResult> {
  return fetchApi(`/api/actors/${tmdbId}/filmography`, signal);
}
