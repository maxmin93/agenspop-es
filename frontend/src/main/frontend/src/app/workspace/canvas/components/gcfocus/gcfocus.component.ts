import { Component, OnInit, Input, ViewChild, ElementRef, AfterViewInit, OnDestroy } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';
import { CY_STYLES } from 'src/app/utils/cy-styles';
import { ICON_PREFIX } from 'src/app/utils/palette-colors';
import { IElement } from 'src/app/models/agens-graph-types';
import { element } from 'protractor';

declare const cytoscape:any;
declare const tippy:any;
declare const jQuery:any;

const CY_CONFIG:any ={
  // initial viewport state:
  zoom: 1.7,
  pan: { x:0, y:0 },
  minZoom: 1e-2,
  maxZoom: 1e1,
  wheelSensitivity: 0.2,
  boxSelectionEnabled: true,
  motionBlur: true,
  selectionType: "single",
  // autoungrabify: true        // cannot move node by user control
}

@Component({
  selector: 'app-gcfocus',
  templateUrl: './gcfocus.component.html',
  styleUrls: ['./gcfocus.component.css']
})
export class GcFocusComponent implements OnInit, AfterViewInit, OnDestroy {

  @Input() public root:any; // root node of graph-contraction
  @Input() public cy:any;   // parent cytoscape

  jsons: IElement[] = [];
  groot: any = undefined;   // re-created root node
  gcy: any = undefined;     // child cytoscape

  @ViewChild("gcy", {read: ElementRef, static: false}) divCy: ElementRef;

  constructor(public modal: NgbActiveModal) { }

  ngOnInit() {
    // console.log('  - root:', this.root.json());
    this.jsons = this.getElementsJson(this.root);
  }
  ngAfterViewInit(){
    this.cyInit(this.jsons);
  }
  ngOnDestroy(){
    this.jsons = [];
    this.groot = undefined;
    if( this.gcy ){
      if( !this.gcy.destroyed() ) this.gcy.destroy();
    }
    this.gcy = window['gcy'] = undefined;
  }

  copyJson(e:any){
    let json = e.json();
    let element:IElement = {
      group: json.group,
      data: json.data,
      scratch: e.scratch()
    };
    if(json.hasOwnProperty('classes')) element['classes'] = json.classes;
    return element;
  }

  getChildrenRecursive(eles:any){
    if( eles && eles.size() == 0 ) return null;
    let expansion = eles.filter(e=>e.scratch('_child_nodes') && e.scratch('_child_nodes').size()>0).toArray();
    for( let e of expansion ){
      let children = this.getChildrenRecursive(e.scratch('_child_nodes'));
      if( children ) eles = eles.union(children);
    }
    return eles;
  }

  getElementsJson(root:any):IElement[] {
    let jsons = [];
    jsons.push( this.copyJson(root) );
    let nodes = root.scratch('_child_nodes') ? this.getChildrenRecursive( root.scratch('_child_nodes') ) : null;
    // console.log('getElementsJson:', children);
    if( nodes ){
      nodes.forEach(e=>jsons.push( this.copyJson(e) ));
      nodes = nodes.union(root);
      let edges = nodes.edgesWith(nodes);
      edges.forEach(e=>jsons.push( this.copyJson(e) ));
    }
    return jsons;
  }

  cyInit(jsons:IElement[]){
    let config:any = Object.assign( _.cloneDeep(CY_CONFIG), {
      container: this.divCy.nativeElement,
      elements: this.jsons,
      style: CY_STYLES,
      ready: (e)=>{
        let cy = e.cy;
        cy.nodes().forEach(e => this.setStyleNode(e));
        cy.edges().forEach(e => this.setStyleEdge(e));
        this.groot = cy.getElementById(this.root.id());

        let layout = cy.layout({ name: "concentric", fit: true, padding: 40,
          startAngle: 3 / 2 * Math.PI, avoidOverlap: true, minNodeSpacing: 10,
          concentric:
            (node)=>{
              return (this.root && this.root.id()==node.id()) ? 9 : node.scratch('_rank');
            }
        });
        layout.run();
      }
    });

    cytoscape.warnings(false);                 // ** for PRODUCT
    this.gcy = window['gcy'] = cytoscape(config);
  }

  private setStyleNode(e:any){
    // e.ungrabify();
    if( e.scratch('_color') ){
      e.style('background-color', e.scratch('_color'));
    }
    if( e.scratch('_icon') ){
      e.style('background-image', ICON_PREFIX+e.scratch('_icon').path);
      e.style('border-opacity', 0);
      e.addClass('icon');
    }
    else{
      e.style('background-image', null);
      e.style('border-opacity', 1);
      e.removeClass('icon');
    }
    e.style('label', '['+e.data('label')+']\n'+e.id());
    e.style('text-opacity', 0.4);
    e.style('text-wrap', 'wrap');
    e.style('font-size', 8);
  }

  private setStyleEdge(e:any){
    if( e.scratch('_color') && e.scratch('_color').length == 2 ){
      e.style('target-arrow-color', e.scratch('_color')[1]);
      e.style('line-gradient-stop-colors', e.scratch('_color'));
    }
  }

}
