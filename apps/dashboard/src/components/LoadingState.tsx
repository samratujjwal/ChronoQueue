export function LoadingState({ label }: { label: string }) {
  return (
    <div className="state-block" role="status" aria-live="polite">
      <div className="spinner" aria-hidden="true" />
      <p>{label}</p>
    </div>
  );
}
