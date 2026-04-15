import { useState } from 'react';
import { useAppState } from '../../context/hooks';
import { useShortestPath } from '../../hooks/useShortestPath';
import { SearchBar } from '../SearchBar/SearchBar';
import { PathChain } from './PathChain';

interface ActorPick {
  tmdb_id: number;
  name: string;
}

export function PathFinder() {
  const { selectedActor, secondActor } = useAppState();
  const { result, loading, error, findPath, clear } = useShortestPath();

  const [actor1, setActor1] = useState<ActorPick | null>(selectedActor);
  const [actor2, setActor2] = useState<ActorPick | null>(secondActor);

  const pick1 = actor1 ?? selectedActor;
  const pick2 = actor2 ?? secondActor;

  const canSearch = pick1 && pick2 && pick1.tmdb_id !== pick2.tmdb_id;

  return (
    <div className="p-6 h-full overflow-y-auto">
      <div className="flex items-center gap-3 mb-6 flex-wrap">
        <SearchBar
          actorsOnly
          placeholder="Actor 1..."
          initialValue={pick1?.name ?? ''}
          className="min-w-40"
          onActorSelect={(a) => { setActor1(a); clear(); }}
          onClear={() => { setActor1(null); clear(); }}
        />
        <span className="text-xl text-muted-foreground">&rarr;</span>
        <SearchBar
          actorsOnly
          placeholder="Actor 2..."
          initialValue={pick2?.name ?? ''}
          className="min-w-40"
          onActorSelect={(a) => { setActor2(a); clear(); }}
          onClear={() => { setActor2(null); clear(); }}
        />
        <button
          className="px-5 py-2.5 bg-primary text-white font-semibold text-sm rounded-lg transition-colors whitespace-nowrap hover:bg-primary-hover disabled:opacity-50 disabled:cursor-not-allowed"
          disabled={!canSearch || loading}
          onClick={() => {
            if (canSearch) findPath(pick1.tmdb_id, pick2.tmdb_id);
          }}
        >
          {loading ? 'Searching...' : 'Find Path'}
        </button>
      </div>

      {error && (
        <div className="px-4 py-3 bg-red-500/15 border border-red-500/30 rounded-lg text-red-300 text-[13px] mb-4">
          {error}
        </div>
      )}

      {result && (
        <div className="mt-2">
          {result.found ? (
            <>
              <div className="text-center mb-6">
                <span className="block text-5xl font-bold text-primary">
                  {result.degrees}
                </span>
                <span className="text-sm text-muted-foreground">
                  degree{result.degrees !== 1 ? 's' : ''} of separation
                </span>
              </div>
              <PathChain path={result.path} />
            </>
          ) : (
            <div className="text-center p-6 text-muted-foreground text-sm">
              {result.message}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
