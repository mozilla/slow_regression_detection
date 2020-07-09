library(feather)
library(tidyverse)
# library(data.table)
library(brms)
# library(bayesplot)
# library(tidybayes)

source("stan_utility.r")


args <- commandArgs(trailingOnly = TRUE)
load_dir <- args[1]
print(load_dir)
# Call like `Rscript smod.r ../../data/cage/suite_data`
setwd(load_dir)

# setwd("/Users/wbeard/repos/perf/data/interim/")


thin <- function(df, n=4) {
  df[seq(1, nrow(df), n), ]
}

get_pis <- function(mod, all.days) {
  pi1 <- predictive_interval(mod, newdata=all.days, prob = .9)
  pi2 <- predictive_interval(mod, newdata=all.days, prob = .7)
  pi3 <- predictive_interval(mod, newdata=all.days, prob = .5)
  pi4 <- predictive_interval(mod, newdata=all.days, prob = .68)
  pi5 <- predictive_interval(mod, newdata=all.days, prob = .01)
  cbind(pi1, pi2, pi3, pi4, pi5)
}

read.df <- function(pth) {
  df = read_feather(pth) %>% as.data.frame()
  df = df[!df$out,]
   # %>% subset(select = c("dayi"))
  df
}

# f = bf(y ~ s(dayi, k=28, bs='cr'))
# , sigma ~ rstd
f = bf(y ~ s(dayi, k=28, bs='cr'))

files <- Sys.glob("suite_data/ts*")
df0 <- read.df(files[1])

fit_empty <- brm(f, df0, chains = 0)

for (filename in files) {
  print(filename)
  df <- read.df(filename)
  all.days <- data.frame(dayi = seq(min(df$dayi), to = max(df$dayi)))

  {
    sink("/dev/null");
    model = update(fit_empty, newdata=df, recompile = FALSE, cores = 4);
    sink();
  }

  check_all_diagnostics(model$fit)
  pi <- get_pis(model, all.days)
  draws <- brms::posterior_predict(model, newdata = all.days)
  draws <- thin(draws)

  fn2 = paste0('brpi/', basename(filename))
  fn.draws = paste0('br_draws/', basename(filename))
  print("=>")
  print(fn2)
  write_feather(data.frame(pi), fn2)
  write_feather(data.frame(draws), fn.draws)

}



# fit1 = update(fit_empty, newdata=df, recompile = FALSE, cores = 4)
# fit2 = update(fit_empty, newdata=df, recompile = FALSE, cores = 4)

# marginal_effects(fit1)

# Sys.sleep(30)
