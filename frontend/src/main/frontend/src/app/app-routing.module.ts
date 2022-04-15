import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { DashboardComponent } from './dashboard/dashboard.component';
import { WorkspaceComponent } from './workspace/workspace.component';
// import { WorkspaceGardService } from "./services/workspace-gard.service";

const routes: Routes = [
    {
        path: 'dashboard',
        component: DashboardComponent
    },
    {
        path: 'workspace/:ds', data: { mode: 'canvas', debug: false },
        component: WorkspaceComponent
    },
    {
        path: 'workspace',
        component: WorkspaceComponent
    },
    {
        path: '', redirectTo: '/workspace', pathMatch: 'full'
    },
];

@NgModule({
    imports: [RouterModule.forRoot(routes)],
    exports: [RouterModule]
})
export class AppRoutingModule {

}
