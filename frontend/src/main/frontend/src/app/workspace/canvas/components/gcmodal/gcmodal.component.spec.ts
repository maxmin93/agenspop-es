import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { GcModalComponent } from './gcmodal.component';

describe('GcconfigComponent', () => {
  let component: GcModalComponent;
  let fixture: ComponentFixture<GcModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ GcModalComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(GcModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
