const TMDB_IMAGE_BASE = 'https://image.tmdb.org/t/p';

export function profileUrl(path: string | null, size = 'w185'): string | null {
  return path ? `${TMDB_IMAGE_BASE}/${size}${path}` : null;
}

export function posterUrl(path: string | null, size = 'w342'): string | null {
  return path ? `${TMDB_IMAGE_BASE}/${size}${path}` : null;
}
