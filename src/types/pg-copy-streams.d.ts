declare module "pg-copy-streams" {
  import { Writable } from "node:stream";

  export function from(sql: string): Writable;
}
