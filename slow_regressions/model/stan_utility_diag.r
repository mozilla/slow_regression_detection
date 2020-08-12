# Based on Michael Betancourt's
# https://raw.githubusercontent.com/betanalpha/knitr_case_studies/master/rstan_workflow/stan_utility.R
library(rstan)


check_div <- function(fit) {
  sampler_params <- get_sampler_params(fit, inc_warmup=FALSE)
  divergent <- do.call(rbind, sampler_params)[,'divergent__']
  n = sum(divergent)
  n
}

check_rhat <- function(fit) {
  "Hopefully less than 1.1"
  'if > 1.1, chains very likely have not mixed'

  rh = brms::rhat(fit)
  max(rh)
}

check_treedepth <- function(fit, max_depth = 10) {
  "Want n = 0"
  sampler_params <- get_sampler_params(fit, inc_warmup=FALSE)
  treedepths <- do.call(rbind, sampler_params)[,'treedepth__']
  n = length(treedepths[sapply(treedepths, function(x) x == max_depth)])
  # N = length(treedepths)
  n
}

check_energy <- function(fit) {
  "Want this above .2"
  sampler_params <- get_sampler_params(fit, inc_warmup=FALSE)
  min_bfmi <- 100
  for (n in 1:length(sampler_params)) {
    energies = sampler_params[n][[1]][,'energy__']
    numer = sum(diff(energies)**2) / length(energies)
    denom = var(energies)
    ratio <- numer / denom
    min_bfmi <- min(ratio, min_bfmi)
  }
  min_bfmi
}


# Checks the effective sample size per iteration
check_n_eff <- function(fit) {
  "n_eff / iter below 0.001 indicates that the effective sample size has likely been overestimated"
  fit_summary <- summary(fit, probs = c(0.5))$summary
  N <- dim(fit_summary)[[1]]

  iter <- dim(extract(fit)[[1]])[[1]]

  min_ratio <- 1
  for (n in 1:N) {
    ratio <- fit_summary[,5][n] / iter
    min_ratio <- min(ratio, min_ratio)

  }
  min_ratio
}


check <- function(fit) {
  ndiv <- check_div(fit)
  n_eff <- check_n_eff(fit)
  max_rhat <- check_rhat(fit)
  exceeded_max_treedepth <- check_treedepth(fit)
  min_energy <- check_energy(fit)


  list(n_divergent=ndiv, n_eff_ratio=n_eff, max_rhat=max_rhat,
       exceeded_max_treedepth=exceeded_max_treedepth,
       min_energy=min_energy
      )
}

# Returns parameter arrays separated into divergent and non-divergent transitions
partition_div <- function(fit) {
  nom_params <- extract(fit, permuted=FALSE)
  n_chains <- dim(nom_params)[2]
  params <- as.data.frame(do.call(rbind, lapply(1:n_chains, function(n) nom_params[,n,])))

  sampler_params <- get_sampler_params(fit, inc_warmup=FALSE)
  divergent <- do.call(rbind, sampler_params)[,'divergent__']
  params$divergent <- divergent

  div_params <- params[params$divergent == 1,]
  nondiv_params <- params[params$divergent == 0,]

  return(list(div_params, nondiv_params))
}
