// Calculate est shares to complete the current round
avg_shares_to_solve = function(difficulty){
  return difficulty * Math.pow(2, 16);
}
// Calculate estimated payout for next round
round_payout = function(difficulty, user_shares, shares_to_solve, donate, round_reward){
  user_percentage = user_shares / shares_to_solve;
  return ((user_percentage * round_reward)) * (1 - donate);
}
// Calculate est coins per day
daily_est = function(last_10_shares, shares_to_solve, donate, round_reward) {
  day_shares = (last_10_shares * 6 * 24);
  daily_percentage = (day_shares / shares_to_solve);
  return ((daily_percentage * round_reward)) * (1 - donate);
}
// Calculate number of shares in pplns
shares_in_pplns = function(shares_to_solve, n_multiplier) {
  return shares_to_solve * n_multiplier;
}