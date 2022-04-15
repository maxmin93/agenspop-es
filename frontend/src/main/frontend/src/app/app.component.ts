import { Component } from '@angular/core';

import { ApApiService } from './services/ap-api.service';

@Component({
    selector: 'app-root',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.css']
})
export class AppComponent {
    title = 'ap-workspace';

    constructor(
        private apApiService: ApApiService,
    ) {
        // this.apApiService.loadConfig().subscribe(x => {	// callback
        //     if( x.hasOwnProperty('debug') && x['debug'] ) console.log('** config:', x);
        //     Object.keys(x).forEach(key=>localStorage.setItem(key,x[key]));    // save value as string
        // });
    }
}
