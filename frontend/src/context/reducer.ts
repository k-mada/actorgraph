import { type AppState, type AppAction } from "../types";

export const initialState: AppState = {
  selectedActor: null,
  secondActor: null,
  detailPanel: null,
};

export function reducer(state: AppState, action: AppAction): AppState {
  switch (action.type) {
    case "SELECT_ACTOR":
      return {
        ...state,
        selectedActor: action.actor,
        detailPanel: { type: "actor", tmdb_id: action.actor.tmdb_id },
      };
    case "SELECT_SECOND_ACTOR":
      return { ...state, secondActor: action.actor };
    case "OPEN_DETAIL":
      return { ...state, detailPanel: action.detail };
    case "CLOSE_DETAIL":
      return { ...state, detailPanel: null };
    case "CLEAR":
      return initialState;
  }
}
