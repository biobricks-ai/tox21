library(arrow)
library(tidyverse)

# tox21lib with reference data on substances
tox21lib <- readr::read_tsv("raw/tox21_10k_library_info.tsv/tox21_10k_library_info.tsv")
arrow::write_parquet(tox21lib,"brick/tox21lib.parquet")

# aggregated files
aggfiles <- fs::dir_ls("raw",recurse=T,regexp="aggregrated.txt")
aggtable <- map(aggfiles,~readr::read_tsv(.x))
aggtable <- keep(aggtable,~nrow(.x)>0)
aggtable <- map(aggtable, ~ mutate(., SAMPLE_DATA_TYPE = as.character(SAMPLE_DATA_TYPE)))
aggmerge <- bind_rows(aggtable)
arrow::write_parquet(aggmerge,"brick/tox21_aggregated.parquet")

# raw files
rawfiles <- fs::dir_ls("raw",recurse=T,regexp="^raw/tox21.*.txt")
rawfiles <- discard(rawfiles,~grepl("aggregrated",.x))
rawfiles <- discard(rawfiles,~grepl("description",.x))
rawtable <- map(rawfiles,~readr::read_tsv(.x))
rawtable <- keep(rawtable,~nrow(.x)>0)
rawmerge <- bind_rows(rawtable)
arrow::write_parquet(aggmerge,"brick/tox21.parquet")