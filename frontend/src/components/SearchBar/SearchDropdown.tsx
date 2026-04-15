import { memo } from "react";
import type { SearchResults } from "../../types";
import { useAppDispatch } from "../../context/hooks";
import { profileUrl, posterUrl } from "../../utils/images";

interface Props {
  results: SearchResults | null;
  loading: boolean;
  onSelect: (name?: string) => void;
  actorsOnly?: boolean;
  onActorSelect?: (actor: { tmdb_id: number; name: string }) => void;
  onMovieSelect?: (movie: { tmdb_id: number; title: string }) => void;
}

const DROPDOWN_CLS =
  "absolute top-[calc(100%+4px)] left-0 right-0 bg-surface border border-border rounded-lg max-h-[400px] overflow-y-auto z-50";
const ROW_CLS =
  "flex items-center gap-2.5 w-full px-4 py-2 text-left hover:bg-muted-surface transition-colors";
const THUMB_CIRCLE_CLS =
  "w-8 h-8 rounded-full object-cover bg-muted-surface flex items-center justify-center text-[13px] font-semibold text-muted-foreground shrink-0";
const THUMB_POSTER_CLS =
  "w-6 h-9 rounded-[3px] object-cover bg-muted-surface shrink-0";
const ROW_NAME_CLS = "text-sm overflow-hidden text-ellipsis whitespace-nowrap";

export const SearchDropdown = memo(function SearchDropdown({
  results,
  loading,
  onSelect,
  actorsOnly,
  onActorSelect,
  onMovieSelect,
}: Props) {
  const dispatch = useAppDispatch();

  if (loading && !results) {
    return (
      <div className={DROPDOWN_CLS}>
        <div className="px-4 py-3 text-muted-foreground text-[13px]">
          Searching...
        </div>
      </div>
    );
  }

  if (!results) return null;

  return (
    <div className={DROPDOWN_CLS}>
      {results.actors.length > 0 && (
        <div className="py-1">
          <div className="px-4 pt-1.5 pb-1 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            Actors
          </div>
          {results.actors.map((actor) => (
            <button
              key={actor.tmdb_id}
              className={ROW_CLS}
              onClick={() => {
                if (onActorSelect) {
                  onActorSelect({ tmdb_id: actor.tmdb_id, name: actor.name });
                } else {
                  dispatch({
                    type: "SELECT_ACTOR",
                    actor: { tmdb_id: actor.tmdb_id, name: actor.name },
                  });
                }
                onSelect(actor.name);
              }}
            >
              {actor.profile_path ? (
                <img
                  className={THUMB_CIRCLE_CLS}
                  src={profileUrl(actor.profile_path, "w45")!}
                  alt=""
                />
              ) : (
                <span className={THUMB_CIRCLE_CLS}>{actor.name[0]}</span>
              )}
              <span className={ROW_NAME_CLS}>{actor.name}</span>
            </button>
          ))}
        </div>
      )}

      {!actorsOnly && results.movies.length > 0 && (
        <div className="py-1 border-t border-border">
          <div className="px-4 pt-1.5 pb-1 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            Movies
          </div>
          {results.movies.map((movie) => {
            const imgSrc =
              movie.poster_url ?? posterUrl(movie.poster_path, "w92");
            return (
              <button
                key={movie.tmdb_id}
                className={ROW_CLS}
                onClick={() => {
                  if (onMovieSelect) {
                    onMovieSelect({ tmdb_id: movie.tmdb_id, title: movie.title });
                  } else {
                    dispatch({
                      type: "OPEN_DETAIL",
                      detail: { type: "movie", tmdb_id: movie.tmdb_id },
                    });
                  }
                  onSelect(movie.title);
                }}
              >
                {imgSrc ? (
                  <img className={THUMB_POSTER_CLS} src={imgSrc} alt="" />
                ) : (
                  <span className={THUMB_POSTER_CLS} />
                )}
                <span className={ROW_NAME_CLS}>
                  {movie.title}
                  {movie.release_year && (
                    <span className="text-muted-foreground">
                      {" "}
                      ({movie.release_year})
                    </span>
                  )}
                </span>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
});
