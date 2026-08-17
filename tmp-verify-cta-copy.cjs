fetch("https://www.dealality.com/old-home?v=" + Date.now())
  .then((r) => r.text())
  .then((t) => {
    const checks = [
      "How Much Hotel Value Are You",
      "Leaving on the Table?",
      "Most hotel owners don't lose value",
      "Discover My Best Strategy",
      "Built by hotel development professionals",
      "See My Untapped Value",
      "No account required to begin",
    ];
    for (const k of checks) console.log(t.includes(k) ? "YES" : "NO ", k);
  });
