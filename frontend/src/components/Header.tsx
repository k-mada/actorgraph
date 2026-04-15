import { SearchBar } from "./SearchBar/SearchBar";
import { Link, useNavigate } from "react-router-dom";
import { useAppDispatch } from "../context/hooks";

const TABS = [
  { key: "graph" as const, label: "Graph" },
  { key: "path" as const, label: "Path Finder" },
  { key: "common-movies" as const, label: "Common Movies" },
];

const Header = () => {
  const navigate = useNavigate();
  const dispatch = useAppDispatch();

  const handleActorSelect = (actor: { tmdb_id: number; name: string }) => {
    dispatch({ type: "SELECT_ACTOR", actor });
    navigate("/graph");
  };

  const handleMovieSelect = (movie: { tmdb_id: number; title: string }) => {
    dispatch({ type: "OPEN_DETAIL", detail: { type: "movie", tmdb_id: movie.tmdb_id } });
    navigate("/graph");
  };

  return (
    <header className="flex items-center gap-4 px-5 py-3 bg-surface border-b border-border z-10">
      <h1 className="text-lg font-bold whitespace-nowrap text-primary">
        Actor Graph
      </h1>
      <SearchBar onActorSelect={handleActorSelect} onMovieSelect={handleMovieSelect} />
      <nav className="flex gap-1 ml-auto shrink-0">
        {TABS.map((tab) => (
          <Link
            to={`/${tab.key}`}
            key={tab.key}
            className="px-3.5 py-1.5 rounded-lg text-[13px] font-medium text-muted-foreground hover:bg-muted-surface hover:text-foreground transition-colors"
          >
            {tab.label}
          </Link>
        ))}
      </nav>
    </header>
  );
};

export default Header;
