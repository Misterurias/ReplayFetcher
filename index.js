import { Scraper } from "./scraper.js";

const scraper = new Scraper();

process.on("SIGINT",  () => scraper.stop("SIGINT"));
process.on("SIGTERM", () => scraper.stop("SIGTERM"));

scraper.run().catch((err) => {
    console.error("[FATAL]", err);
    process.exit(1);
});