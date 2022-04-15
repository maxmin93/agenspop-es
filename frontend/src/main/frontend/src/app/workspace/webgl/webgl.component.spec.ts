import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { WebglComponent } from './webgl.component';
import { HttpClientModule } from '@angular/common/http';

describe('WebglComponent', () => {
  let component: WebglComponent;
  let fixture: ComponentFixture<WebglComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ WebglComponent ],
      imports: [ HttpClientModule ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(WebglComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
