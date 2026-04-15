// import type { SearchResults } from "../types";
import { searchAll } from "../services/api";
import { useQuery } from "@tanstack/react-query";

const useSearch = (query: string) => {
  const { data, isLoading, error } = useQuery({
    queryKey: ["search", query],
    queryFn: ({ signal }) => searchAll(query, signal),
    enabled: query.length >= 2,
  });

  return {
    results: data ?? null,
    loading: isLoading,
    error: error?.message ?? null,
  };
};

export default useSearch;
