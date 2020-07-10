library(feather)
library(tidyverse)
library(brms)

# # library(data.table)
# # library(bayesplot)
# # library(tidybayes)

# library(here)


getScriptPath <- function(){
    cmd.args <- commandArgs()
    m <- regexpr("(?<=^--file=).+", cmd.args, perl=TRUE)
    script.dir <- dirname(regmatches(cmd.args, m))
    if(length(script.dir) == 0) stop("can't determine script dir: please call the script with Rscript")
    if(length(script.dir) > 1) stop("can't determine script dir: more than one '--file' argument detected")
    return(script.dir)
}

here.loc = getScriptPath()
source(paste0(here.loc, '/', "stan_utility.r"))


args <- commandArgs(trailingOnly = TRUE)
load_dir <- args[1]
print(paste('load_dir', load_dir))
# Call like `Rscript slow_regressions/model/smod.r /tmp/brms/`
# so that "suite_data/ts*"
setwd(load_dir)

# setwd("/Users/wbeard/repos/perf/data/interim/")


thin <- function(df, n=4) {
  df[seq(1, nrow(df), n), ]
}

models <- list()
get.knots <- function(df) {
    ceiling(max(df$dayi) / 8)
}

library(glue)
get.model <- function(df) {
  get.knots <- function(df) {
    ceiling(max(df$dayi) / 8)
  }
  
  k <- get.knots(df)
  print(paste0('pulling model with ', k, ' knots'))
  key <- paste0('k', k)
  if (!is_null(models[[key]])){
    return(models[[key]])
  }
  
  print(k)
  print('going...')
  form_s <- glue("y ~ s(dayi, k={k}, bs='cr')")
  f <- bf(form_s)
  
  models[[key]] <<- brm(f, df, chains = 0)
  models[[key]]
}

# get.model_ <- function(df) {  
#   k <- get.knots(df)
#   print(paste0('pulling model with ', k, ' knots'))
#   key <- paste0('k', k)
#   if (!is_null(models[[key]])){
#     return(models[[key]])
#   }

#   k <- get.knots(df)
#   f <- bf(y ~ s(dayi, k=k, bs='cr'))
#   brm(f, df, chains = 0)
#   models[[key]] <<- brm(f, df, chains = 0)
#   models[[key]]
# }

# get.model2 <- function(df) {
#   k <- 42
#   f <- bf(y ~ s(dayi, k=k, bs='cr'))
#   brm(f, df, chains = 0)
# }

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
# f = bf(y ~ s(dayi, k=28, bs='cr'))

files <- Sys.glob("suite_data/ts*")

# df0 <- read.df(files[1])
# fit_empty <- brm(f, df0, chains = 0)

for (filename in files) {
  print(filename)
  df <- read.df(filename)
  fit_empty <- get.model(df)

  # k <- get.knots(df)
  # print(paste0('pulling model with ', k, ' knots'))
  # key <- paste0('k', k)
  # if (is_null(models[[key]])){
  #   f <- bf(y ~ s(dayi, k=k, bs='cr'))
  #   models[[key]] <- brm(f, df, chains = 0)
  # }

  # fit_empty <- models[[key]]
  # fit_empty <- get.model(df)
  # fit_empty <- get.model2(df)

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
