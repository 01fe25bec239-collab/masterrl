/** Fixed-size buffer that keeps the newest `capacity` items and counts what it pushed out. */
export class RingBuffer<T> {
  private readonly items: (T | undefined)[];
  private start = 0;
  private length = 0;
  /** Items pushed out because the buffer was full. */
  evicted = 0;

  constructor(readonly capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new Error('A ring buffer needs a capacity of at least 1.');
    }
    this.items = new Array<T | undefined>(capacity);
  }

  get size(): number {
    return this.length;
  }

  push(item: T): void {
    const end = (this.start + this.length) % this.capacity;
    this.items[end] = item;
    if (this.length < this.capacity) this.length += 1;
    else {
      this.start = (this.start + 1) % this.capacity;
      this.evicted += 1;
    }
  }

  /** Oldest first. */
  toArray(): T[] {
    const out: T[] = [];
    for (let i = 0; i < this.length; i += 1) {
      out.push(this.items[(this.start + i) % this.capacity] as T);
    }
    return out;
  }

  clear(): void {
    this.items.fill(undefined);
    this.start = 0;
    this.length = 0;
    this.evicted = 0;
  }
}
