import { BrowserRouter as Router, Routes, Route, Navigate } from "react-router-dom";
import { GraphView } from "./components/GraphView/GraphView";
import { PathFinder } from "./components/PathFinder/PathFinder";
import { CommonMovies } from "./components/CommonMovies/CommonMovies";
import MainLayout from "./components/MainLayout";
import { QueryClientProvider, QueryClient } from "@tanstack/react-query";

export default function App() {
  const queryClient = new QueryClient();

  return (
    <QueryClientProvider client={queryClient}>
      <Router>
        <Routes>
          <Route path="/" element={<MainLayout />}>
            <Route index element={<Navigate to="/graph" replace />} />
            <Route path="/graph" element={<GraphView />} />
            <Route path="/path" element={<PathFinder />} />
            <Route path="/common-movies" element={<CommonMovies />} />
          </Route>
        </Routes>
      </Router>
    </QueryClientProvider>
  );
}
