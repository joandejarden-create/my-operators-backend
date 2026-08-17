
import { scoreDevRecommendationLab } from "../lib/ai-visibility/classifier-lab/score-dev.js";
const s = await scoreDevRecommendationLab({ classifierVersion: process.argv[2] });
process.stdout.write(JSON.stringify(s));
