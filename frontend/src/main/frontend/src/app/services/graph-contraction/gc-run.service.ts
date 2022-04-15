import { GcStrategy } from './gc-strategy';
import { GcCentralityStrategy } from './gc-centrality.strategy';
import { GcLabelAllStrategy } from './gc-labelall.strategy';
import { GcLabelValueStrategy } from './gc-labelvalue.strategy';

export class GcRunService {

  private strategy: GcStrategy;
  private topRank: number;
  private currRank: number;

  constructor(cy:any, option:any){
    // create strategy for user option
    if( option.method == 'centrality' )
      this.strategy = new GcCentralityStrategy(cy, option.data);
    else{
      if( option.data.key && option.data.key !== '_all_' )
        this.strategy = new GcLabelValueStrategy(cy, option.data.label, option.data.key);
      else
        this.strategy = new GcLabelAllStrategy(cy, option.data.label);
    }

    // init rank counters
    this.topRank = this.strategy.getTopRank();
    this.currRank = -1;

    if( window['gc'] ) window['gc']['currRank'] = this.currRank;
  }

  clear(){
    this.strategy.doClear();
  }

  contraction(){
    if( this.currRank+1 >= this.topRank ) return;
    this.currRank += 1;

    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('graph::contraction', this.currRank);

    this.strategy.doContraction(this.currRank);
  }

  expansion(){
    if( this.currRank < 0 ) return;

    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('graph::expansion', this.currRank);

    this.strategy.doExpansion(this.currRank);
    this.currRank -= 1;
  }

}