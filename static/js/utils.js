function numberWithCommas(x) {
    var parts = x.toString().split(".");
    parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    return parts.join(".");
}
// Calculate est shares to complete the current round
avg_shares_to_solve = function(difficulty){
  return difficulty * window.shares_per_hash;
}
// Calculate estimated payout for next round
round_payout = function(difficulty, user_shares, shares_to_solve, donate, round_reward, n_multiplier, pplns_total_shares){
  var user_percentage;
  if (pplns_total_shares < (shares_to_solve * n_multiplier)) {
    user_percentage = user_shares / pplns_total_shares;
  } else {
    user_percentage = user_shares / (shares_to_solve * n_multiplier);
  }
  return ((user_percentage * round_reward)) * (1 - (donate/100));
}
// Calculate est coins per day
daily_est = function(last_10_shares, shares_to_solve, donate, round_reward) {
  var day_shares, daily_percentage;
  day_shares = (last_10_shares * 6 * 24);
  daily_percentage = (day_shares / shares_to_solve);
  return ((daily_percentage * round_reward)) * (1 - (donate/100));
}
// Calculate number of shares in pplns
shares_in_pplns = function(pplns_window, n_multiplier) {
  return pplns_window * n_multiplier;
}
