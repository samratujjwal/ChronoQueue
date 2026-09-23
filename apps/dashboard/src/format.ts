export function formatDate(value: string | null): string {
  if (!value) {
    return "—";
  }
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString();
}

export function shortId(id: string): string {
  return id.length > 8 ? id.slice(0, 8) : id;
}
