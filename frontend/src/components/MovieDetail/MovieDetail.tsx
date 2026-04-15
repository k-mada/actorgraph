import { useState, useEffect } from "react";
import type { Movie } from "../../types";
import { getMovie } from "../../services/api";
import { useAppDispatch } from "../../context/hooks";
import { posterUrl, profileUrl } from "../../utils/images";

interface Props {
  tmdbId: number;
}

export function MovieDetail({ tmdbId }: Props) {
  const dispatch = useAppDispatch();
  const [movie, setMovie] = useState<Movie | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    setMovie(null);
    getMovie(tmdbId)
      .then(setMovie)
      .finally(() => setLoading(false));
  }, [tmdbId]);

  if (loading) {
    return (
      <div className="p-10 text-center text-muted-foreground">Loading...</div>
    );
  }

  if (!movie) return null;

  const poster = movie.poster_url ?? posterUrl(movie.poster_path);
  const year = movie.release_date?.split("-")[0];

  return (
    <div className="relative">
      <button
        className="absolute top-3 right-3 text-[22px] text-white/80 px-2 py-1 rounded z-[1] [text-shadow:0_1px_3px_rgba(0,0,0,0.6)] hover:bg-black/30 hover:text-white"
        onClick={() => dispatch({ type: "CLOSE_DETAIL" })}
      >
        &times;
      </button>

      {poster && (
        <img className="w-full block" src={poster} alt={movie.title} />
      )}

      <div className="px-5 py-4">
        <h2 className="text-xl font-bold mb-2">
          {movie.title}
          {year && (
            <span className="text-muted-foreground font-normal"> ({year})</span>
          )}
        </h2>

        {movie.vote_average > 0 && (
          <span className="inline-block px-2 py-0.5 rounded bg-warning text-black text-[13px] font-bold mb-3">
            {movie.vote_average.toFixed(1)}
          </span>
        )}

        {movie.genres.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mb-3">
            {movie.genres.map((g) => (
              <span
                key={g}
                className="px-2.5 py-0.5 rounded-xl bg-muted-surface text-xs text-muted-foreground"
              >
                {g}
              </span>
            ))}
          </div>
        )}

        {movie.overview && (
          <p className="text-[13px] leading-relaxed text-muted-foreground mb-5">
            {movie.overview}
          </p>
        )}

        <div className="text-sm font-semibold pb-2 border-b border-border mb-2">
          Cast ({movie.cast.length})
        </div>

        <div className="flex flex-col gap-0.5">
          {movie.cast.map((member) => {
            const thumb = profileUrl(member.profile_path, "w45");
            return (
              <button
                key={member.tmdb_id}
                className="flex items-center gap-2.5 px-1 py-1.5 rounded text-left hover:bg-muted-surface transition-colors"
                onClick={() =>
                  dispatch({
                    type: "OPEN_DETAIL",
                    detail: { type: "actor", tmdb_id: member.tmdb_id },
                  })
                }
              >
                {thumb ? (
                  <img
                    className="w-10 h-10 rounded-full object-cover bg-muted-surface shrink-0"
                    src={thumb}
                    alt=""
                  />
                ) : (
                  <div className="w-10 h-10 rounded-full bg-muted-surface flex items-center justify-center text-sm font-semibold text-muted-foreground shrink-0">
                    <span>{member.name[0]}</span>
                  </div>
                )}
                <div className="flex flex-col gap-px min-w-0">
                  <span className="text-[13px] font-medium">{member.name}</span>
                  {member.character && (
                    <span className="text-xs text-muted-foreground">
                      as {member.character}
                    </span>
                  )}
                </div>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
