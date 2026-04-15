import { createContext, type Dispatch } from "react";
import { type AppState, type AppAction } from "../types";
import { initialState } from "./reducer";

export const AppStateContext = createContext<AppState>(initialState);
export const AppDispatchContext = createContext<Dispatch<AppAction>>(() => {});
