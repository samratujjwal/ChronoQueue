export function EmptyState({ title, hint }: { title: string; hint?: string }) {
  return (
    <div className="state-block">
      <p className="state-title">{title}</p>
      {hint ? <p className="state-hint">{hint}</p> : null}
    </div>
  );
}
