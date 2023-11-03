/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React from 'react';
import { render } from '@testing-library/react';
import ProcessDefinitionListContainer from '../ProcessDefinitionListContainer';
import * as ProcessDefinitionListContext from '../../../../channel/ProcessDefinitionList/ProcessDefinitionListContext';
import { ProcessDefinitionListGatewayApiImpl } from '../../../../channel/ProcessDefinitionList/ProcessDefinitionListGatewayApi';
import DevUIAppContextProvider from '../../../contexts/DevUIAppContextProvider';
import {
  DefaultUser,
  User
} from '@kogito-apps/consoles-common/dist/environment/auth';
import { EmbeddedProcessDefinitionList } from '@kogito-apps/process-definition-list';

jest
  .spyOn(ProcessDefinitionListContext, 'useProcessDefinitionListGatewayApi')
  .mockImplementation(
    () =>
      new ProcessDefinitionListGatewayApiImpl(
        'http://localhost:9000',
        '/mocked'
      )
  );

const user: User = new DefaultUser('jon', []);
const appContextProps = {
  devUIUrl: 'http://localhost:9000',
  openApiPath: '/mocked',
  isProcessEnabled: false,
  isTracingEnabled: false,
  omittedProcessTimelineEvents: [],
  availablePages: [],
  customLabels: {
    singularProcessLabel: 'test-singular',
    pluralProcessLabel: 'test-plural'
  },
  diagramPreviewSize: { width: 100, height: 100 }
};

describe('ProcessDefinitionListContainer tests', () => {
  it('Snapshot', () => {
    const { container } = render(
      <DevUIAppContextProvider users={[user]} {...appContextProps}>
        <ProcessDefinitionListContainer />
      </DevUIAppContextProvider>
    );

    expect(container).toMatchSnapshot();

    expect(container.querySelector('div')).toBeTruthy();
  });
});
