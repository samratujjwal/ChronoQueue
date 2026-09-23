import { useEffect, useState } from "react";
import { Overview } from "./pages/Overview.js";
import { Jobs } from "./pages/Jobs.js";
import { JobDetail } from "./pages/JobDetail.js";
import { DeadQueue } from "./pages/DeadQueue.js";

type Route =
  | { name: "overview" }
  | { name: "jobs" }
  | { name: "jobDetail"; id: string }
  | { name: "dead" };

function parseHash(): Route {
  const hash = window.location.hash.replace(/^#/, "") || "/";
  if (hash === "/") {
    return { name: "overview" };
  }
  if (hash === "/jobs") {
    return { name: "jobs" };
  }
  if (hash === "/dead") {
    return { name: "dead" };
  }
  const match = /^\/jobs\/([^/]+)$/.exec(hash);
  if (match?.[1]) {
    return { name: "jobDetail", id: decodeURIComponent(match[1]) };
  }
  return { name: "overview" };
}

function NavLink({ href, label, active }: { href: string; label: string; active: boolean }) {
  return (
    <a href={href} className={active ? "nav-link nav-active" : "nav-link"} aria-current={active ? "page" : undefined}>
      {label}
    </a>
  );
}

export function App() {
  const [route, setRoute] = useState<Route>(() => parseHash());

  useEffect(() => {
    function onHashChange() {
      setRoute(parseHash());
    }
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  return (
    <div className="app">
      <header className="topbar">
        <a href="#/" className="brand">
          ChronoQueue
        </a>
        <nav className="nav">
          <NavLink href="#/" label="Overview" active={route.name === "overview"} />
          <NavLink href="#/jobs" label="Jobs" active={route.name === "jobs" || route.name === "jobDetail"} />
          <NavLink href="#/dead" label="Dead Queue" active={route.name === "dead"} />
        </nav>
      </header>

      <main className="content">
        {route.name === "overview" ? <Overview /> : null}
        {route.name === "jobs" ? <Jobs /> : null}
        {route.name === "jobDetail" ? <JobDetail key={route.id} id={route.id} /> : null}
        {route.name === "dead" ? <DeadQueue /> : null}
      </main>

      <footer className="footer">
        This dashboard currently uses the same unauthenticated API posture as
        the existing ChronoQueue API. Authentication/authorization is outside
        the scope of Day 16.
      </footer>
    </div>
  );
}
