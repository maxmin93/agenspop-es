import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { NgModule } from '@angular/core';
import { HttpClientModule } from '@angular/common/http';
import { FormsModule } from '@angular/forms';

import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxTypeaheadModule } from 'ngx-typeahead';
import { NgxSpinnerModule } from "ngx-spinner";

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';

import { HttpClientInMemoryWebApiModule } from 'angular-in-memory-web-api';
import { DataSrcService } from './services/fake-api/data-src.service';
import { WorkspaceComponent } from './workspace/workspace.component';
import { HeaderComponent } from './workspace/header/header.component';
import { PropertyComponent } from './workspace/property/property.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { CanvasComponent } from './workspace/canvas/canvas.component';
import { WebglComponent } from './workspace/webgl/webgl.component';

import { ContextMenuModule } from 'ngx-contextmenu';
import { GcModalComponent } from './workspace/canvas/components/gcmodal/gcmodal.component';
import { GcFocusComponent } from './workspace/canvas/components/gcfocus/gcfocus.component';

@NgModule({
  declarations: [
    AppComponent,
    WorkspaceComponent,
    HeaderComponent,
    PropertyComponent,
    DashboardComponent,
    CanvasComponent,
    WebglComponent,
    GcModalComponent,
    GcFocusComponent,

    // ExpandComponent,
    // FilterComponent,
    // DesignComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    
    NgbModule,
    FormsModule,
    AppRoutingModule,
    HttpClientModule,

    NgxTypeaheadModule,
    NgxSpinnerModule,
    ContextMenuModule.forRoot({useBootstrap4: true})
		// HttpClientInMemoryWebApiModule.forRoot(
		// 	DataSrcService, {dataEncapsulation: false}
		// )
  ],
  providers: [],
  entryComponents: [ GcModalComponent, GcFocusComponent ],
  bootstrap: [ AppComponent ]
})
export class AppModule { }
