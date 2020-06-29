import React from 'react';
import Form from '../../../../components/Form/Form';
import Header from '../../../Header';
import Joi from 'joi-browser';
import Dropdown from 'react-bootstrap/Dropdown';
import { post, get } from '../../../../utils/api';
import { formatDateTime } from '../../../../utils/converters';
import {
  uriTopicsProduce,
  uriTopicsPartitions,
  uriPreferredSchemaForTopic
} from '../../../../utils/endpoints';
import moment from 'moment';
import DatePicker from '../../../../components/DatePicker';

class TopicProduce extends Form {
  state = {
    partitions: [],
    nHeaders: 1,
    formData: {
      partition: '',
      key: '',
      hKey0: '',
      hValue0: '',
      value: ''
    },
    datetime: new Date(),
    openDateModal: false,
    errors: {},
    selectableValueFormats: [
      { _id: 'string', name: 'string' },
      { _id: 'json', name: 'json' }
    ],
    keySchema: [],
    keySchemaSearchValue: '',
    selectedKeySchema: '',
    valueSchema: [],
    valueSchemaSearchValue: '',
    selectedValueSchema: '',
    clusterId: '',
    topicId: ''
  };

  schema = {
    partition: Joi.number()
      .min(0)
      .label('Partition')
      .required(),
    key: Joi.string()
      .allow('')
      .label('Key'),
    hKey0: Joi.string()
      .allow('')
      .label('hKey0'),
    hValue0: Joi.string()
      .allow('')
      .label('hValue0'),
    value: Joi.string()
      .allow('')
      .label('Value')
  };

  async componentDidMount() {
    const { clusterId, topicId } = this.props.match.params;
    this.props.history.replace({
      ...this.props.location,
      loading: true
    });
    try {
      let response = await get(uriTopicsPartitions(clusterId, topicId));
      let partitions = response.data.map(item => {
        return { name: item.id, _id: Number(item.id) };
      });
      this.setState({
        partitions,
        formData: {
          ...this.state.formData,
          partition: partitions[0]._id
        }
      });
    } catch (err) {
      console.error('err', err);
    } finally {
      this.props.history.replace({
        ...this.props.location,
        loading: false
      });
    }
    this.getPreferredSchemaForTopic();
    this.setState({ clusterId, topicId });
  }

  async getPreferredSchemaForTopic() {
    const { clusterId, topicId } = this.props.match.params;
    const { history } = this.props;
    try {
      let schema = await get(uriPreferredSchemaForTopic(clusterId, topicId));
      let keySchema = [];
      let valueSchema = [];
      (schema.data && schema.data.key) && schema.data.key.map(index => keySchema.push(index));
      (schema.data && schema.data.value) && schema.data.value.map(index => valueSchema.push(index));
      this.setState({ keySchema: keySchema, valueSchema: valueSchema });
    } catch (err) {
      if (err.status === 404) {
        history.replace('/ui/page-not-found', { errorData: err });
      } else {
        history.replace('/ui/error', { errorData: err });
      }
    }
  }

  doSubmit() {
    const {
      formData,
      datetime,
      selectedKeySchema,
      selectedValueSchema,
      keySchema,
      valueSchema
    } = this.state;
    const { clusterId, topicId } = this.props.match.params;

    let schemaKeyToSend = keySchema.find(key => key.subject === selectedKeySchema);
    let schemaValueToSend = valueSchema.find(value => value.subject === selectedValueSchema);
    const topic = {
      clusterId,
      topicId,
      partition: formData.partition,
      key: formData.key,
      timestamp: datetime.toISOString(),
      value: JSON.parse(JSON.stringify(formData.value)),
      keySchema: schemaKeyToSend ? schemaKeyToSend.id : '',
      valueSchema: schemaValueToSend ? schemaValueToSend.id : ''
    };

    let headers = {};
    Object.keys(formData).forEach(key => {
      if (key.includes('hKey')) {
        let keyNumbers = key.replace(/\D/g, '');
        headers[formData[key]] = formData[`hValue${keyNumbers}`];
      }
    });

    topic.headers = headers;
    this.props.history.replace({
      ...this.props.location,
      loading: true
    });
    post(uriTopicsProduce(clusterId, topicId), topic)
      .then(() => {
        this.props.history.push({
          ...this.props.location,
          pathname: `/ui/${clusterId}/topic/${topicId}`,
          showSuccessToast: true,
          successToastMessage: `Produced to ${topicId}.`,
          loading: false
        });
      })
      .catch(err => {
        console.log('err', err);
        this.props.history.replace({
          ...this.props.location,
          showErrorToast: true,
          errorToastMessage: err.message || 'There was an error while producing to topic.',
          loading: false
        });
      });
  }

  renderHeaders() {
    let headers = [];

    Object.keys(this.state.formData).forEach(key => {
      if (key.includes('hKey')) {
        let keyNumbers = key.replace(/\D/g, '');
        headers.push(this.renderHeader(Number(keyNumbers)));
      }
    });
    return <div data-testId="headers">{headers.map(head => head)}</div>;
  }

  renderHeader(position) {
    return (
      <div className="row header-wrapper">
        <label className="col-sm-2 col-form-label">{position === 0 ? 'Header' : ''}</label>

        <div className="row col-sm-10 khq-multiple header-row">
          {this.renderInput(
            `hKey${position}`,
            '',
            'Key',
            'text',
            undefined,
            true,
            'wrapper-class col-sm-6 col-5 key',
            'input-class'
          )}

          {this.renderInput(
            `hValue${position}`,
            '',
            'Value',
            'text',
            undefined,
            true,
            'wrapper-class col-sm-6 col-5',
            'input-class'
          )}
          <div className="add-button">
            <button
              type="button"
              className="btn btn-secondary"
              data-testId={`button_${position}`}
              onClick={() => {
                position === 0 ? this.handlePlus() : this.handleRemove(position);
              }}
            >
              <i class={`fa ${position === 0 ? 'fa-plus' : 'fa-trash'}`}></i>
            </button>
          </div>
        </div>
      </div>
    );
  }

  handlePlus() {
    const { formData, nHeaders } = this.state;

    let newFormData = {
      ...formData,
      [`hKey${nHeaders}`]: '',
      [`hValue${nHeaders}`]: ''
    };
    this.schema = {
      ...this.schema,
      [`hKey${nHeaders}`]: Joi.string()
        .min(1)
        .label(`hKey${nHeaders}`),
      [`hValue${nHeaders}`]: Joi.string()
        .min(1)
        .label(`hValue${nHeaders}`)
    };
    this.setState({ nHeaders: this.state.nHeaders + 1, formData: newFormData });
  }

  handleRemove(position) {
    const state = {
      ...this.state
    };
    delete state.formData[`hKey${position}`];
    delete state.formData[`hValue${position}`];
    this.setState(state);
  }

  renderResults = (results, searchValue, selectedValue, tag) => {
    return (
      <div style={{ maxHeight: '678px', overflowY: 'auto', minHeight: '89px' }}>
        <ul
          className="dropdown-menu inner show"
          role="presentation"
          style={{ marginTop: '0px', marginBottom: '0px' }}
        >
          {results
            .filter(key => {
              if (searchValue.length > 0) {
                return key.includes(searchValue);
              }
              return results;
            })
            .map((key, index) => {
              let selected = selectedValue === key ? 'selected' : '';
              return (
                <li>
                  <div
                    onClick={e => {
                      if (tag === 'key') {
                        selectedValue === key
                          ? this.setState({ selectedKeySchema: '' })
                          : this.setState({ selectedKeySchema: key });
                      } else if (tag === 'value') {
                        selectedValue === key
                          ? this.setState({ selectedValueSchema: '' })
                          : this.setState({ selectedValueSchema: key });
                      }
                    }}
                    role="option"
                    className={`dropdown-item ${selected}`}
                    id={`bs-select-${index}-0`}
                    aria-selected="false"
                  >
                    <span className="text">{key}</span>
                  </div>
                </li>
              );
            })}
        </ul>{' '}
      </div>
    );
  };

  render() {
    const {
      topicId,
      formData,
      partitions,
      datetime,
      selectableValueFormats,
      keySchema,
      keySchemaSearchValue,
      selectedKeySchema,
      valueSchema,
      valueSchemaSearchValue,
      selectedValueSchema
    } = this.state;
    let date = moment(datetime);
    return (
      <div style={{ overflow: 'hidden', paddingRight: '20px', marginRight: 0 }}>
        <form encType="multipart/form-data" className="khq-form khq-form-config">
          <div>
            <Header title={`Produce to ${topicId} `} />
            {this.renderSelect('partition', 'Partition', partitions, value => {
              this.setState({ formData: { ...formData, partition: value.target.value } });
            })}
            {this.renderDropdown(
              'Key schema',
              keySchema.map(key => key.subject),
              keySchemaSearchValue,
              selectedKeySchema,
              value => {
                this.setState({
                  keySchemaSearchValue: value.target.value,
                  selectedKeySchema: value.target.value
                });
              },
              this.renderResults(
                keySchema.map(key => key.subject),
                keySchemaSearchValue,
                selectedKeySchema,
                'key'
              )
            )}
            {this.renderInput('key', 'Key', 'Key', 'Key')}
            <div></div>
          </div>

          {this.renderHeaders()}
          {this.renderSelect('valueFormat', 'Value Format', selectableValueFormats, value => {
            if (value.target.value === 'string') {
              this.schema.value = Joi.string().label('Value');
              this.forceUpdate();
            } else if (value.target.value === 'json') {
              this.schema.value = Joi.any().label('Value');
              this.forceUpdate();
            }
          })}
          {this.renderDropdown(
            'Value schema',
            valueSchema.map(value => value.subject),
            valueSchemaSearchValue,
            selectedValueSchema,
            value => {
              this.setState({
                valueSchemaSearchValue: value.target.value,
                selectedValueSchema: value.target.value
              });
            },
            this.renderResults(
              valueSchema.map(value => value.subject),
              valueSchemaSearchValue,
              selectedValueSchema,
              'value'
            )
          )}
          {this.renderJSONInput('value', 'Value', value => {
            this.setState({
              formData: {
                ...formData,
                value: value
              }
            });
          })}
          <div style={{ display: 'flex', flexDirection: 'row', width: '100%', padding: 0 }}>
            <label
              style={{ padding: 0, alignItems: 'center', display: 'flex' }}
              className="col-sm-2 col-form-label"
            >
              Timestamp:
            </label>
            <Dropdown style={{ width: '100%', padding: 0, margin: 0 }}>
              <Dropdown.Toggle
                style={{
                  width: '100%',
                  display: 'flex',
                  flexDirection: 'row',
                  alignItems: 'center',
                  padding: 0,
                  margin: 0
                }}
                className="nav-link dropdown-toggle"
              >
                <input
                  className="form-control"
                  value={
                    datetime !== '' &&
                    ' ' +
                      formatDateTime(
                        {
                          year: date.year(),
                          monthValue: date.month() + 1,
                          dayOfMonth: date.date(),
                          hour: date.hour(),
                          minute: date.minute(),
                          second: date.second()
                        },
                        'DD-MM-YYYY HH:mm'
                      )
                  }
                  placeholder={
                    datetime !== '' &&
                    ' ' +
                      formatDateTime(
                        {
                          year: date.year(),
                          monthValue: date.month() + 1,
                          dayOfMonth: date.date(),
                          hour: date.hour(),
                          minute: date.minute(),
                          second: date.second()
                        },
                        'DD-MM-YYYY HH:mm'
                      )
                  }
                />
              </Dropdown.Toggle>
              <Dropdown.Menu>
                <div className="input-group">
                  <DatePicker
                    showDateTimeInput
                    showTimeInput
                    value={datetime}
                    onChange={value => {
                      this.setState({
                        datetime: value
                      });
                    }}
                  />
                </div>
              </Dropdown.Menu>
            </Dropdown>
          </div>

          {this.renderButton(
            'Produce',
            () => {
              this.doSubmit();
            },
            undefined,
            'button'
          )}
        </form>
      </div>
    );
  }
}

export default TopicProduce;
