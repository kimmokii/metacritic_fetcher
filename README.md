# Metacritic Fetcher

Metacritic Fetcher is a small collection of TypeScript scripts for downloading, cleaning, and merging movie review data from Metacritic.
The project is designed to support a larger analysis pipeline where raw review data is fetched from the website, patched to fix missing critic information, and merged into a consistent final dataset.

All processing steps are modular and can be run independently.

## Features

###  Fetch reviews from Metacritic
A scraping script that downloads critic and user review data for a selected movie and exports it to JSON.

###  Patch missing critic information
Some Metacritic entries lack critic names or metadata. A dedicated patch script fills in these missing fields using a manually curated JSON file.

###  Merge patched and original datasets
A merge script combines the patched critic data back into the original review list, producing a cleaned final dataset ready for analysis.

###  Lightweight & script-based
All processing stages live under the `scripts/` directory and can be executed with `ts-node`.

## Repository Structure

```
metacritic_fetcher/
├── scripts/
│   ├── export-movie-reviews.ts
│   ├── patch_missing_critics.ts
│   ├── merge_patched_data.ts
├── data/ (ignored)
│   ├── movie_reviews_raw.json
│   ├── critic_missing.json
│   ├── movie_reviews_patched.json
│   └── movie_reviews_final.json
└── README.md
```

## Installation

```
npm install
```

## Usage

### Run the scraper

```
ts-node scripts/export-movie-reviews.ts
```

### Patch missing critics

```
ts-node scripts/patch_missing_critics.ts
```

### Merge datasets

```
ts-node scripts/merge_patched_data.ts
```

## Output

- `movie_reviews_raw.json` — raw scraped data
- `movie_reviews_patched.json` — patched data
- `movie_reviews_final.json` — merged final dataset

## Motivation

Metacritic critic data is inconsistent. This toolkit provides a reproducible way to fetch data, detect missing critic metadata, patch inconsistencies, and produce a final dataset for analysis.

## Requirements

- Node.js (v18+ recommended)
- TypeScript
- ts-node

## License

This project is licensed under the MIT License.
