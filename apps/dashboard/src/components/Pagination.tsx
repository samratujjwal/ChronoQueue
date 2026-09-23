export function Pagination({
  page,
  totalPages,
  onPage,
}: {
  page: number;
  totalPages: number;
  onPage: (page: number) => void;
}) {
  if (totalPages <= 1) {
    return null;
  }
  return (
    <div className="pagination">
      <button
        type="button"
        disabled={page <= 1}
        onClick={() => onPage(page - 1)}
        aria-label="Previous page"
      >
        ← Prev
      </button>
      <span className="pagination-info">
        Page {page} of {totalPages}
      </span>
      <button
        type="button"
        disabled={page >= totalPages}
        onClick={() => onPage(page + 1)}
        aria-label="Next page"
      >
        Next →
      </button>
    </div>
  );
}
