import Header from "../components/Header";
import { Outlet } from "react-router-dom";

const MainLayout = () => {
  return (
    <div className="flex flex-col h-screen bg-background">
      <Header />
      <main className="flex-1 min-h-0 relative overflow-auto">
        <Outlet />
      </main>
    </div>
  );
};

export default MainLayout;
