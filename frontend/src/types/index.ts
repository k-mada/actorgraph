// Search results from GET /api/search
export interface SearchResults {
  actors: SearchActor[];
  movies: SearchMovie[];
}
export interface SearchActor {
  tmdb_id: number;
  name: string;
  profile_path: string | null;
  popularity: number;
  type: "actor";
  in_database: boolean;
}
export interface SearchMovie {
  tmdb_id: number;
  title: string;
  poster_path: string | null;
  poster_url: string | null;
  release_year: number | null;
  popularity: number;
  type: "movie";
  in_database: boolean;
}

// Actor detail from GET /api/actors/{id}
export interface Actor {
  tmdb_id: number;
  name: string;
  profile_path: string | null;
  popularity: number;
  birthday: string | null;
  biography: string | null;
  place_of_birth: string | null;
  fully_fetched: boolean;
  fetched_at: string;
}

// Movie detail from GET /api/movies/{id}
export interface Movie {
  tmdb_id: number;
  title: string;
  release_date: string;
  release_year: number | null;
  poster_path: string | null;
  poster_url: string | null;
  overview: string;
  popularity: number;
  vote_average: number;
  genres: string[];
  fetched_at: string;
  cast: CastMember[];
}
export interface CastMember {
  tmdb_id: number;
  name: string;
  profile_path: string | null;
  character: string | null;
  billing_order: number | null;
}

// Graph data from GET /api/graph/neighborhood/{id}
export interface GraphData {
  nodes: GraphNode[];
  links: GraphLink[];
  center_tmdb_id?: number;
}
export interface GraphNode {
  id: number;
  name: string;
  profile_path: string | null;
  popularity: number;
  type: "actor";
  is_center: boolean;
}
export interface GraphLink {
  source: number;
  target: number;
  shared_movies: SharedMovie[];
  weight: number;
}
export interface SharedMovie {
  tmdb_id: number;
  title: string;
  release_date: string;
  release_year: number | null;
  poster_path: string | null;
  poster_url: string | null;
  vote_average: number;
}

// Path result from GET /api/actors/{id}/path/{id2}
export type PathResult = PathFound | PathNotFound;
export interface PathFound {
  actor1_tmdb_id: number;
  actor2_tmdb_id: number;
  found: true;
  degrees: number;
  path: PathStep[];
}
export interface PathNotFound {
  actor1_tmdb_id: number;
  actor2_tmdb_id: number;
  found: false;
  message: string;
}
export type PathStep =
  | {
      type: "actor";
      tmdb_id: number;
      name: string;
      profile_path: string | null;
    }
  | {
      type: "movie";
      tmdb_id: number;
      title: string;
      poster_path: string | null;
      poster_url: string | null;
      release_date: string;
    };

// Common movies from GET /api/actors/{id}/common-movies/{id2}
export interface CommonMoviesResult {
  actor1_tmdb_id: number;
  actor2_tmdb_id: number;
  common_movies: CommonMovie[];
  count: number;
}
export interface CommonMovie {
  tmdb_id: number;
  title: string;
  release_date: string;
  poster_path: string | null;
  poster_url: string | null;
  vote_average: number;
  actor1_character: string | null;
  actor2_character: string | null;
}

// Filmography from GET /api/actors/{id}/filmography
export interface FilmographyResult {
  actor_tmdb_id: number;
  movies: FilmographyMovie[];
}
export interface FilmographyMovie {
  tmdb_id: number;
  title: string;
  release_date: string;
  release_year: number | null;
  poster_path: string | null;
  poster_url: string | null;
  vote_average: number;
  character: string | null;
}

// State management
export interface ActorRef {
  tmdb_id: number;
  name: string;
}

export interface DetailPanel {
  type: "actor" | "movie";
  tmdb_id: number;
}

export interface AppState {
  selectedActor: ActorRef | null;
  secondActor: ActorRef | null;
  detailPanel: DetailPanel | null;
}

export type AppAction =
  | { type: "SELECT_ACTOR"; actor: ActorRef }
  | { type: "SELECT_SECOND_ACTOR"; actor: ActorRef }
  | { type: "OPEN_DETAIL"; detail: DetailPanel }
  | { type: "CLOSE_DETAIL" }
  | { type: "CLEAR" };
