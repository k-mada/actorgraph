import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import type { Actor, FilmographyMovie } from "../../types";
import { getActor, getActorFilmography } from "../../services/api";
import { useAppDispatch } from "../../context/hooks";
import { profileUrl, posterUrl } from "../../utils/images";

interface Props {
  tmdbId: number;
}

export function ActorDetail({ tmdbId }: Props) {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const [actor, setActor] = useState<Actor | null>(null);
  const [movies, setMovies] = useState<FilmographyMovie[]>([]);
  const [loading, setLoading] = useState(true);
  const [bioExpanded, setBioExpanded] = useState(false);

  useEffect(() => {
    setLoading(true);
    setActor(null);
    setMovies([]);
    setBioExpanded(false);

    Promise.all([getActor(tmdbId), getActorFilmography(tmdbId)])
      .then(([actorData, filmData]) => {
        setActor(actorData);
        setMovies(filmData.movies);
      })
      .finally(() => setLoading(false));
  }, [tmdbId]);

  if (loading) {
    return (
      <div className="p-10 text-center text-muted-foreground">Loading...</div>
    );
  }

  if (!actor) return null;

  const imgSrc = profileUrl(actor.profile_path, "w342");
  const bio = actor.biography ?? "";
  const showReadMore = bio.length > 200;
  const displayBio = bioExpanded ? bio : bio.slice(0, 200);

  return (
    <div className="p-5 relative">
      <button
        className="absolute top-3 right-3 text-[22px] text-muted-foreground px-2 py-1 rounded hover:bg-muted-surface hover:text-foreground"
        onClick={() => dispatch({ type: "CLOSE_DETAIL" })}
      >
        &times;
      </button>

      <div className="flex justify-center mb-4">
        {imgSrc ? (
          <img
            className="w-30 h-30 rounded-full object-cover bg-muted-surface"
            src={imgSrc}
            alt={actor.name}
          />
        ) : (
          <div className="w-30 h-30 rounded-full bg-muted-surface flex items-center justify-center">
            <span className="text-[40px] font-semibold text-muted-foreground">
              {actor.name[0]}
            </span>
          </div>
        )}
      </div>

      <h2 className="text-xl font-bold text-center mb-2">{actor.name}</h2>

      {(actor.birthday || actor.place_of_birth) && (
        <p className="text-[13px] text-muted-foreground text-center mb-3">
          {actor.birthday && <>Born: {actor.birthday}</>}
          {actor.birthday && actor.place_of_birth && " — "}
          {actor.place_of_birth}
        </p>
      )}

      {bio && (
        <p className="text-[13px] leading-normal text-muted-foreground mb-4">
          {displayBio}
          {showReadMore && (
            <button
              className="text-primary text-[13px] hover:text-primary-hover"
              onClick={() => setBioExpanded(!bioExpanded)}
            >
              {bioExpanded ? " Show less" : "... Read more"}
            </button>
          )}
        </p>
      )}

      <div className="flex gap-2 mb-5">
        <button
          className="flex-1 px-3 py-2 text-[13px] font-semibold rounded-lg bg-primary text-white hover:bg-primary-hover transition-colors"
          onClick={() =>
            dispatch({
              type: "SELECT_ACTOR",
              actor: { tmdb_id: actor.tmdb_id, name: actor.name },
            })
          }
        >
          Explore Graph
        </button>
        <button
          className="flex-1 px-3 py-2 text-[13px] font-semibold rounded-lg border border-border text-foreground hover:bg-muted-surface transition-colors"
          onClick={() => {
            dispatch({ type: "SELECT_SECOND_ACTOR", actor: { tmdb_id: actor.tmdb_id, name: actor.name } });
            navigate("/path");
          }}
        >
          Find Path To...
        </button>
      </div>

      <div className="text-sm font-semibold pb-2 border-b border-border mb-2">
        Filmography ({movies.length})
      </div>

      <div className="flex flex-col gap-0.5">
        {movies.map((movie) => {
          const poster =
            movie.poster_url ?? posterUrl(movie.poster_path, "w92");
          return (
            <button
              key={movie.tmdb_id}
              className="flex items-center gap-2.5 px-1 py-1.5 rounded text-left hover:bg-muted-surface transition-colors"
              onClick={() =>
                dispatch({
                  type: "OPEN_DETAIL",
                  detail: { type: "movie", tmdb_id: movie.tmdb_id },
                })
              }
            >
              {poster ? (
                <img
                  className="w-11.25 h-16.75 rounded object-cover bg-muted-surface shrink-0"
                  src={poster}
                  alt=""
                />
              ) : (
                <div className="w-11.25 h-16.75 rounded bg-muted-surface shrink-0" />
              )}
              <div className="flex flex-col gap-0.5 min-w-0">
                <span className="text-[13px] font-medium overflow-hidden text-ellipsis whitespace-nowrap">
                  {movie.title}
                </span>
                <span className="text-xs text-muted-foreground">
                  {movie.release_year}
                  {movie.character && <> &middot; {movie.character}</>}
                </span>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
