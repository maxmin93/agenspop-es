import { TestBed } from '@angular/core/testing';

import { DataApiService } from './data-api.service';
import { HttpClientModule } from '@angular/common/http';

describe('DataApiService', () => {
  beforeEach(() => TestBed.configureTestingModule({
    imports: [ HttpClientModule ],
  }));

  it('should be created', () => {
    const service: DataApiService = TestBed.get(DataApiService);
    expect(service).toBeTruthy();
  });
});
