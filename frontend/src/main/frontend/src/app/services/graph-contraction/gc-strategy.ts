export interface GcStrategy {
  getTopRank():number;

  // contraction
  doContraction(currRank:number);

  // extraction
  doExpansion(currRank:number);

  // before close
  doClear();
}