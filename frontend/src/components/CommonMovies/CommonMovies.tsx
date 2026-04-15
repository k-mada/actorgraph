import { useState, useCallback } from "react";
import type { CommonMoviesResult } from "../../types";
import { getCommonMovies } from "../../services/api";
import { useAppState, useAppDispatch } from "../../context/hooks";
import { posterUrl } from "../../utils/images";
import { SearchBar } from "../SearchBar/SearchBar";

interface ActorPick {
  tmdb_id: number;
  name: string;
}

export function CommonMovies() {
  const { selectedActor, secondActor } = useAppState();
  const dispatch = useAppDispatch();
  const [data, setData] = useState<CommonMoviesResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [actor1, setActor1] = useState<ActorPick | null>(selectedActor);
  const [actor2, setActor2] = useState<ActorPick | null>(secondActor);

  const pick1 = actor1 ?? selectedActor;
  const pick2 = actor2 ?? secondActor;

  const fetchCommon = useCallback((a: ActorPick, b: ActorPick) => {
    if (a.tmdb_id === b.tmdb_id) return;
    setLoading(true);
    setError(null);
    setData(null);
    getCommonMovies(a.tmdb_id, b.tmdb_id)
      .then(setData)
      .catch((e) => setError((e as Error).message))
      .finally(() => setLoading(false));
  }, []);

  const handleActor1 = useCallback(
    (a: ActorPick) => {
      setActor1(a);
      setData(null);
      const other = actor2 ?? secondActor;
      if (other) fetchCommon(a, other);
    },
    [actor2, secondActor, fetchCommon],
  );

  const handleActor2 = useCallback(
    (a: ActorPick) => {
      setActor2(a);
      setData(null);
      const other = actor1 ?? selectedActor;
      if (other) fetchCommon(other, a);
    },
    [actor1, selectedActor, fetchCommon],
  );

  return (
    <div className="p-6 h-full overflow-y-auto">
      <div className="flex items-center gap-3 mb-6 flex-wrap">
        <SearchBar
          actorsOnly
          placeholder="Actor 1..."
          initialValue={pick1?.name ?? ""}
          className="min-w-40"
          onActorSelect={handleActor1}
          onClear={() => {
            setActor1(null);
            setData(null);
          }}
        />
        <span className="text-xl text-muted-foreground">&amp;</span>
        <SearchBar
          actorsOnly
          placeholder="Actor 2..."
          initialValue={pick2?.name ?? ""}
          className="min-w-40"
          onActorSelect={handleActor2}
          onClear={() => {
            setActor2(null);
            setData(null);
          }}
        />
      </div>

      {loading && (
        <div className="flex items-center justify-center h-full text-muted-foreground text-[15px]">
          <p>Loading...</p>
        </div>
      )}

      {error && <div className="text-red-300">{error}</div>}

      {!loading && !error && pick1 && pick2 && data && data.count === 0 && (
        <div className="flex items-center justify-center h-full text-muted-foreground text-[15px]">
          <p>No common movies found</p>
        </div>
      )}

      {!loading && data && data.count > 0 && (
        <>
          <h2 className="text-lg font-semibold mb-5">
            {pick1!.name} &amp; {pick2!.name} &mdash; {data.count} Movie
            {data.count !== 1 ? "s" : ""} Together
          </h2>
          <div className="grid grid-cols-[repeat(auto-fill,minmax(200px,1fr))] gap-4">
            {data.common_movies.map((movie) => {
              const poster = movie.poster_url ?? posterUrl(movie.poster_path);
              const year = movie.release_date?.split("-")[0];
              return (
                <button
                  key={movie.tmdb_id}
                  className="flex flex-col bg-surface border border-border rounded-lg overflow-hidden text-left transition-colors hover:border-primary"
                  onClick={() =>
                    dispatch({
                      type: "OPEN_DETAIL",
                      detail: { type: "movie", tmdb_id: movie.tmdb_id },
                    })
                  }
                >
                  {poster ? (
                    <img
                      className="w-full aspect-2/3 object-cover"
                      src={poster}
                      alt=""
                    />
                  ) : (
                    <div className="w-full aspect-2/3 bg-muted-surface" />
                  )}
                  <div className="px-3 py-2.5 flex flex-col gap-0.5">
                    <span className="text-sm font-semibold">{movie.title}</span>
                    <span className="text-xs text-muted-foreground flex items-center gap-1.5">
                      {year}
                      {movie.vote_average > 0 && (
                        <span className="px-1.5 py-px rounded bg-warning text-black font-bold text-[11px]">
                          {movie.vote_average.toFixed(1)}
                        </span>
                      )}
                    </span>
                    {movie.actor1_character && (
                      <span className="text-xs text-muted-foreground italic">
                        as {movie.actor1_character}
                      </span>
                    )}
                    {movie.actor2_character && (
                      <span className="text-xs text-muted-foreground italic">
                        as {movie.actor2_character}
                      </span>
                    )}
                  </div>
                </button>
              );
            })}
          </div>
        </>
      )}
    </div>
  );
}
