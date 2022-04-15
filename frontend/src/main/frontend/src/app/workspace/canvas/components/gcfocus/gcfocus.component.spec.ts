import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GcFocusComponent } from './gcfocus.component';

describe('GcfocusComponent', () => {
  let component: GcFocusComponent;
  let fixture: ComponentFixture<GcFocusComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GcFocusComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GcFocusComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
