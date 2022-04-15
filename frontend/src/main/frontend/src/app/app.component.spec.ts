import { TestBed, async, fakeAsync, tick } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { AppComponent } from './app.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { WorkspaceComponent } from './workspace/workspace.component';
import { Router } from '@angular/router';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { Location } from "@angular/common";
import { HttpClientModule } from '@angular/common/http';

describe('AppComponent', () => {
  let location: Location;
  let router: Router;
  let fixture;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientModule,
        RouterTestingModule.withRoutes([
          { path: '', redirectTo: '/workspace', pathMatch: 'full' },
          { path: 'dashboard', component: DashboardComponent },
          { path: 'workspace', component: WorkspaceComponent }
        ])
      ],
      declarations: [
        AppComponent, DashboardComponent, WorkspaceComponent
      ],
      schemas: [ CUSTOM_ELEMENTS_SCHEMA ]
    }).compileComponents();

    router = TestBed.get(Router);
    location = TestBed.get(Location);

    fixture = TestBed.createComponent(AppComponent);
    router.initialNavigation();
  }));

  it('should create the app', () => {
    const fixture = TestBed.createComponent(AppComponent);
    const app = fixture.debugElement.componentInstance;
    expect(app).toBeTruthy();
  });

  it(`should have as title 'ap-workspace'`, () => {
    const fixture = TestBed.createComponent(AppComponent);
    const app = fixture.debugElement.componentInstance;
    expect(app.title).toEqual('ap-workspace');
  });

  it('navigate to "" redirects you to /workspace', fakeAsync(() => {
    router.navigate(['']);
    tick();
    expect(location.path()).toBe('/workspace');
  }));

  // it('should render title', () => {
  //   const fixture = TestBed.createComponent(AppComponent);
  //   fixture.detectChanges();
  //   const compiled = fixture.debugElement.nativeElement;
  //   // expect(compiled.querySelector('.content span').textContent).toContain('ap-workspace app is running!');
  //   expect(router.navigate).toHaveBeenCalledWith(['/workspace'])
  // });
});
