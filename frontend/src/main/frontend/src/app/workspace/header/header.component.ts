import {Component, ElementRef, EventEmitter, OnInit, Output, ViewChild, ViewChildren, Input, ChangeDetectorRef} from '@angular/core';
import { of, fromEvent, Observable, BehaviorSubject} from "rxjs";
import { debounceTime, map, distinctUntilChanged, filter } from "rxjs/operators";

// import {UrState} from "../../ngrx/states/urStates";
// import {Store} from "@ngrx/store";
// import {selectUrState} from "../../ngrx/reducers/ur.reducers";
// import {Neighbour} from "../../models/data-model";

import { NgbModal, ModalDismissReasons } from '@ng-bootstrap/ng-bootstrap';
import { ApApiService } from 'src/app/services/ap-api.service';
import { IGraph, EMPTY_GRAPH } from 'src/app/models/agens-graph-types';
import { ISearch, IEvent } from 'src/app/models/agens-data-types';

declare const jQuery: any;

const SCRIPT_CYPHER:string = 'match (a:customer {country: "Germany"}) return "nodes" as group, { id: id(a), label: labels(a)[0], properties: properties(a) } as data, {} as scratch limit 10';
const SCRIPT_GREMLIN:string =  `g.V().hasLabel('customer').sample(50);
g.V().hasLabel('product').sample(50);
g.V().hasLabel('order').sample(500);`;

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.css']
})
export class HeaderComponent implements OnInit {

	datasources: any[] = [];	// { name, desc }

	private g:IGraph = EMPTY_GRAPH;

	private searchMap = new Map<string,ISearch[]>();
	private searchItem:ISearch = undefined;
	searchResults:ISearch[] = [];
	private isSearching:boolean = false;

	undoable: boolean = false;
	@Input() set changeUndo(state:boolean){
		this.undoable = state;
		// this._cd.detectChanges();
	}
	redoable: boolean = false;
	@Input() set changeRedo(state:boolean){
		this.redoable = state;
		// this._cd.detectChanges();
	}

	nextScreenMode:string;
	@Input() set screenMode(mode:string) {
		this.nextScreenMode = (mode == 'webgl') ? 'canvas' : 'webgl';
	}

  gcMode:boolean=false;   // Drill-down/Roll-up == Graph Constraction
  @Input() set changeGcMode(state:boolean){
    this.gcMode = state;
  }
  @Output() gcEmitter = new EventEmitter<IEvent>();

  private graph$ = new BehaviorSubject<IGraph>(EMPTY_GRAPH);
	@Input() set graph(g:IGraph) { this.graph$.next(g); }

  query:any = { datasource: 'modern', type: 'default', script: undefined };
	@Output() selectQueryEmitter = new EventEmitter<any>();

  @Output() selectSearchEmitter = new EventEmitter<ISearch>();
  @Output() changeScreenModeEmitter = new EventEmitter<string>();
  @Output() undoRedoEmitter= new EventEmitter<string>();
  // @Output() addNodeEmitter = new EventEmitter<null>();
  // @Output() selectAllNodesEmitter = new EventEmitter<null>();
  // @Output() selectLeafNodesEmitter = new EventEmitter<null>();
  // @Output() filterPanelOpen = new EventEmitter<null>();
  // @Output() designPanelOpen = new EventEmitter<null>();

  @ViewChild('ws_header', {read: ElementRef, static: false}) ws_header: ElementRef;
	@ViewChild('searchInput', {read: ElementRef, static: false}) searchInput: ElementRef;

	constructor(/*private urStore: Store<UrState>*/
		private apApiService: ApApiService,
    private modalService: NgbModal,
    private _cd: ChangeDetectorRef        // DANGEROUS!!
	) {
  	// this.urstate = urStore.select(selectUrState);
  }

  ngOnInit() {
  	// this.urstate.subscribe({
    //   next: (urState: UrState) => {
    //     this.undoable = urState.undoable;
    //     this.redoable = urState.redoable;
    //   }
		// });
  }

  ngAfterViewInit() {
  	this.setUpTooltip();
		this.setUpSearch();

		fromEvent(this.searchInput.nativeElement, 'keyup').pipe(
      map((event: any) => {
        return event.target.value;
      })
      //, filter(res => res.length >= 2)		// 길이가 1이면 아예 작동을 안함
      , debounceTime(1200)
      // If previous query is diffent from current
      , distinctUntilChanged()
      // subscription for response
      ).subscribe((query: string) => {
        this.isSearching = true;
				// API: http get (query) => subscribe{ isSearching = false; ... }
				this.searchResults = this.matchSearchMap(query);
				// console.log('searchInput::keyup', query, this.searchResults);
      });

    // Async data (ex: crop to cy-graph)
    this.graph$.subscribe(x => {
      if( !x ) return;
			this.g = <IGraph>x;
			let mapSize = this.buildSearchMap(this.g);
			// console.log('buildSearchMap = '+mapSize);
	  });
  }

	///////////////////////////////////////////////////
	// search datasources by query
	//

	searchDatasources(query:string){
		console.log('search datasources:', query);		// when enter key
		this.apApiService.searchDatasources(query).subscribe(x => {	// callback
			this.datasources = Object.keys(x).map(r => ({name: r, desc: x[r]}) );
		});
	}

	///////////////////////////////////////////////////
	// search elements or values
	// **REF. https://www.freakyjolly.com/angular-simple-typeahead-autocomplete-suggestion-search-implementation-in-angular-6-applications/

	selectedSearch($event){
		this.searchItem = $event;
		this.searchInput.nativeElement.value = $event.key;
		// console.log('selectedSearch.event =>', this.searchItem);
		this.selectSearchEmitter.emit(this.searchItem);
	}

	private buildSearchMap(g:IGraph){
		this.searchMap.clear();
		// add label names
		for( let item of g.labels.nodes )
			this.addSearchItem(item.name, <ISearch>{ ktype: 'name', vtype: 'node-label', value: item});
		for( let item of g.labels.edges )
			this.addSearchItem(item.name, <ISearch>{ ktype: 'name', vtype: 'edge-label', value: item});
		// add nodes with values as string
		for( let item of g.nodes ){
			let keys = Object.keys(item.data.properties);	// convert to string
			for( let k of keys )
				this.addSearchItem(k, <ISearch>{ ktype: 'key', vtype: 'node', value: item});
			let values = Object.values(item.data.properties).map(x=>x+'');	// convert to string
			for( let v of values )
				this.addSearchItem(v, <ISearch>{ ktype: 'value', vtype: 'node', value: item});
		}
		// add edges with values as string
		for( let item of g.edges ){
			let keys = Object.keys(item.data.properties);	// convert to string
			for( let k of keys )
				this.addSearchItem(k, <ISearch>{ ktype: 'key', vtype: 'edge', value: item});
			let values = Object.values(item.data.properties).map(x=>x+'');	// convert to string
			for( let v of values )
				this.addSearchItem(v, <ISearch>{ ktype: 'value', vtype: 'edge', value: item});
		}
		return this.searchMap.size;
	}

	private matchSearchMap(query:string, size:number=10):ISearch[]{
		let result:ISearch[] = [];
		let matchKeys:string[] = [];
		for( let key of this.searchMap.keys() ){
			if( key.indexOf(query) >= 0 ){
				let items = this.searchMap.get(key).map(x=>{ x.key = key; return x; });
				result = result.concat( items );
			}
		}
		return result.slice(0, size);
	}

	private addSearchItem(key:string, item:any){
		if( this.searchMap.has(key) ){
			let items = <any[]>this.searchMap.get(key);
			if( !items.includes(item) ) items.push(item);
		}
		else this.searchMap.set(key, <any[]>[item]);
	}

	///////////////////////////////////////////////////

  private setUpTooltip(){
	  jQuery(this.ws_header.nativeElement).tooltip({
		  container: this.ws_header.nativeElement,
		  selector: '.with_tooltip',
			delay: 250,
			sanitize: false, sanitizeFn: content => content
	  }).on({
		  'show.bs.tooltip'(e: any) {
			  // jQuery(e.target).addClass('show');
		  }
	  });

	  jQuery(this.ws_header.nativeElement).find('.dropdown_wrap').on({
		  'show.bs.dropdown'(e: any) {
			  jQuery(e.target).find('.with_tooltip').tooltip('hide');
			  jQuery(e.target).find('.with_tooltip').tooltip('disable');},
		  'hide.bs.dropdown'(e: any) {
			  jQuery(e.target).find('.with_tooltip').tooltip('toggleEnabled');
		  }
	  });

	  jQuery(this.ws_header.nativeElement).find('#dropdownMenuMore').closest('.dropdown').on({
		  'show.bs.dropdown'() {
			  jQuery('#dropdownMenuSelect').on({
				  'click.bs'(e: any) {
					  e.stopPropagation();
				  }
			  }).closest('.dropright').on({
				  mouseenter(e: any) {
					  jQuery('#dropdownMenuSelect').dropdown('show');
				  },
				  mouseleave(e: any) {
					  jQuery('#dropdownMenuSelect').dropdown('hide');
				  }
			  });
		  },
		  'hide.bs.dropdown'(e: any) {
			  jQuery('#dropdownMenuSelect').off('click.bs');
		  }
	  });
  }

  private setUpSearch() {
	  jQuery('body').on({
		  'click.bs'(e: any) {
			  const $this = jQuery(e.target);
			  if ($this.closest('button').hasClass('btn_reset') && $this.closest('div.ws_search').is(':visible')) { // 검색창
				  const timer = setTimeout(() => {
					  clearTimeout(timer);
					  jQuery('#wsSearch').val('').focus();
				  }, 160);
			  }
		  },
		  'focusin.bs': (e: any) => {
			  const $this = jQuery(e.target);
			  if ( $this.is('input.input_ws') ) { // 검색창
				  $this.closest('.inner').addClass('focusin');
			  }
		  },
		  'focusout.bs': (e: any) => {
			  const $this = jQuery(e.target);
			  if ($this.is('input.input_ws')) { // 검색창
				  const timer = setTimeout(() => {
					  clearTimeout(timer);
					  $this.val('').closest('.inner').removeClass('focusin keyup').off('transitionend webkitTransitionend');
					  $this.closest('.ws_search').find('.result').removeClass('show');
				  }, 150);
			  }
		  },
		  'keyup.bs': (e: any) => {
			  const $this = jQuery(e.target);
			  if ($this.is('input.input_ws')) { // 검색창
				  if ($this.val().length === 0) {
					  $this.closest('.inner').removeClass('keyup').addClass('focusin').off('transitionend webkitTransitionend');
					  $this.closest('.ws_search').find('.result').removeClass('show');
				  } else {
					  $this.closest('.inner').addClass('keyup').removeClass('focusin');
					  $this.closest('.ws_search').find('.result').addClass('show');
				  }
			  }
		  }
	  });
  }

  ///////////////////////////////////////////////////////////////////////

  onChangeScreenMode() {
		this.changeScreenModeEmitter.emit(this.nextScreenMode);
  }

  selectAllNodes() {
	  // this.selectAllNodesEmitter.emit();
  }

  selectLeafNodes() {
  	// this.selectLeafNodesEmitter.emit();
  }

  filterPanelIconClick() {
	  // this.filterPanelOpen.emit();
  }

  designPanelIconClick() {
  	  // this.designPanelOpen.emit();
  }

  undoClick() {
    this.undoRedoEmitter.emit('undo');
  }
  redoClick() {
    this.undoRedoEmitter.emit('redo');
  }

  gcExpansion() {
    this.gcEmitter.emit(<IEvent>{type:'gc',data:'expansion'});
  }
  gcConstraction() {
    this.gcEmitter.emit(<IEvent>{type:'gc',data:'contraction'});
  }

  showFilterPanel(){

  }

  showStylePanel(){

  }

  ///////////////////////////////////////////////////////////////////////

	setQuery(tag:string, val:string){
    if( tag == 'datasource' ) this.query['datasource'] = val;
	}

	openDatasources($modal) {
		this.query = {
			datasource: 'modern',
			type: 'default',		// default, gremlin, cypher
			script: undefined
		};
		this.apApiService.findDatasources().subscribe(x => {	// callback
			this.datasources = Object.keys(x).map(r => ({name: r, desc: x[r]}) );
			this.modalService.open($modal, {ariaLabelledBy: 'modal-basic-title'}).result.then(
				(result) => {
					this.query['datasource'] = result;
					this.selectQueryEmitter.emit(this.query);
				},
				(reason) => { });
		});
	}

	openQueries(type:string, $modal:any) {
		this.query = {
			datasource: 'northwind',
			type: type,
			script: (type=='gremlin' ? SCRIPT_GREMLIN : SCRIPT_CYPHER)
		};
		this.apApiService.findDatasources().subscribe(x => {	// callback
			this.datasources = Object.keys(x).map(r => ({name: r, desc: x[r]}) );
			this.modalService.open($modal, { ariaLabelledBy: 'modal-basic-title', size: 'lg' }).result.then(
				(result)=>{
					this.query['script'] = result;
					this.selectQueryEmitter.emit(this.query);
				},
				(reason)=>{ });
		});
	}

  openGCConfig(){
    this.gcEmitter.emit(<IEvent>{type: 'gc', data: 'config'});
  }

}
