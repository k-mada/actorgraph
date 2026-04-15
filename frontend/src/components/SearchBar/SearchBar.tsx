import {
  useState,
  useRef,
  useEffect,
  useCallback,
  type SyntheticEvent,
} from "react";
import useSearch from "../../hooks/useSearch";
import { SearchDropdown } from "./SearchDropdown";
// import { useAppState } from "../../context/AppContext";

export interface SearchBarProps {
  /** Restrict dropdown to actors only (hides movies). */
  actorsOnly?: boolean;
  /** Custom placeholder text. */
  placeholder?: string;
  /** Pre-fill the input with this value (e.g. a previously selected actor name). */
  initialValue?: string;
  /**
   * Called when an actor is selected from the dropdown.
   * When provided, the SearchBar will NOT dispatch to global state —
   * the parent controls what happens on selection.
   */
  onActorSelect?: (actor: { tmdb_id: number; name: string }) => void;
  /**
   * Called when a movie is selected from the dropdown.
   * When provided, the SearchBar will NOT dispatch to global state —
   * the parent controls what happens on selection.
   */
  onMovieSelect?: (movie: { tmdb_id: number; title: string }) => void;
  /** Called when the user edits the input after a selection, clearing the pick. */
  onClear?: () => void;
  /** Additional CSS class on the outer container. */
  className?: string;
}

function debounce<T extends unknown[], U>({
  callback,
  delay = 300,
}: {
  callback: (...args: T) => PromiseLike<U> | U;
  delay: number;
}) {
  let timer: number;
  return (...args: T): Promise<U> => {
    clearTimeout(timer);
    return new Promise((resolve) => {
      timer = setTimeout(() => resolve(callback(...args)), delay);
    });
  };
}

export function SearchBar({
  actorsOnly = false,
  placeholder = "Search actors or movies...",
  initialValue = "",
  onActorSelect,
  onMovieSelect,
  onClear,
  className,
}: SearchBarProps = {}) {
  const [inputQuery, setInputQuery] = useState(initialValue);
  const [debouncedQuery, setDebouncedQuery] = useState(initialValue);
  const [selected, setSelected] = useState(!!initialValue);
  const [open, setOpen] = useState(false);
  const { results, loading } = useSearch(debouncedQuery);
  const timerRef = useRef<number | undefined>(undefined);
  // const { dispatch } = useAppState();

  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (
        containerRef.current &&
        !containerRef.current.contains(e.target as Node)
      ) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const hasResults =
    results && (results.actors.length > 0 || results.movies.length > 0);
  const showDropdown = open && !selected && (hasResults || loading);

  const handleOnChange = (e: SyntheticEvent<HTMLInputElement>) => {
    clearTimeout(timerRef.current);
    const value = e.currentTarget.value;
    setInputQuery(value);
    timerRef.current = setTimeout(() => {
      setDebouncedQuery(value);
      setOpen(true);
      if (selected) {
        setSelected(false);
        onClear?.();
      }
    }, 300);
  };

  const handleSelect = useCallback((name?: string) => {
    setOpen(false);
    if (name) {
      setInputQuery(name);
      setSelected(true);
    } else {
      setInputQuery("");
    }
  }, []);

  return (
    <div
      className={`relative flex-1 max-w-105 ${className ?? ""}`}
      ref={containerRef}
    >
      <div className="flex items-center gap-2 px-3 py-2 bg-muted-surface border border-border rounded-lg">
        <svg
          className="shrink-0 text-muted-foreground"
          viewBox="0 0 20 20"
          fill="currentColor"
          width="16"
          height="16"
        >
          <path
            fillRule="evenodd"
            d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z"
            clipRule="evenodd"
          />
        </svg>
        <input
          className="w-full text-sm placeholder:text-muted-foreground"
          type="text"
          placeholder={placeholder}
          value={inputQuery}
          onChange={handleOnChange}
          onFocus={() => {
            if (!selected) setOpen(true);
          }}
          onKeyDown={(e) => {
            if (e.key === "Escape") setOpen(false);
          }}
        />
        {loading && (
          <span className="w-3.5 h-3.5 border-2 border-border border-t-primary rounded-full animate-spin shrink-0" />
        )}
      </div>
      {showDropdown && (
        <SearchDropdown
          results={results}
          loading={loading}
          actorsOnly={actorsOnly}
          onActorSelect={onActorSelect}
          onMovieSelect={onMovieSelect}
          onSelect={handleSelect}
        />
      )}
    </div>
  );
}
