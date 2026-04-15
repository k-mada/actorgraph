import { useContext } from "react";
import { AppStateContext, AppDispatchContext } from "./contexts";

export const useAppState = () => useContext(AppStateContext);
export const useAppDispatch = () => useContext(AppDispatchContext);
