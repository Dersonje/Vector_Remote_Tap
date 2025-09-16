import nose2
import json
import subprocess
import requests
import time
import re

class TestVectorConfig:
    def get_component_info(self, name):
        result = subprocess.run(['python', 'vector_script.py', 'get-info', name], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Command failed: {result.stderr}")
        return json.loads(result.stdout)

    def test_sources(self):
        expected_sources = ['my_http_source']
        # To get all sources, we might need to run get-chain on one and extract, but since small, hardcode and check each
        # For simplicity, check the known ones exist by running get-info
        for source in expected_sources:
            info = self.get_component_info(source)
            print(f"Info for {source}: {json.dumps(info, indent=2)}")
            assert 'componentId' in info and info['componentId'] == source, f"Expected componentId {source}, got {info.get('componentId')}"

    def test_transforms(self):
        expected_transforms = ['add_prefix', 'replace_via', 'uppercase_message']
        for transform in expected_transforms:
            info = self.get_component_info(transform)
            print(f"Info for {transform}: {json.dumps(info, indent=2)}")
            assert 'componentId' in info and info['componentId'] == transform, f"Expected componentId {transform}, got {info.get('componentId')}"

    def test_sinks(self):
        expected_sinks = ['my_console_sink']
        for sink in expected_sinks:
            info = self.get_component_info(sink)
            print(f"Info for {sink}: {json.dumps(info, indent=2)}")
            assert 'componentId' in info and info['componentId'] == sink, f"Expected componentId {sink}, got {info.get('componentId')}"

    def test_get_info_my_http_source(self):
        info = self.get_component_info('my_http_source')
        print(f"Info for my_http_source: {json.dumps(info, indent=2)}")
        assert info['componentId'] == 'my_http_source', f"Expected componentId 'my_http_source', got {info['componentId']}"
        assert info['componentType'] == 'http', f"Expected componentType 'http', got {info['componentType']}"
        assert sorted(info['inputs']) == [], f"Expected inputs [], got {sorted(info['inputs'])}"
        assert sorted(info['outputs']) == ['add_prefix', 'transform_4'], f"Expected outputs ['add_prefix', 'transform_4'], got {sorted(info['outputs'])}"
        assert sorted([t['componentId'] for t in info.get('transforms', [])]) == ['add_prefix', 'transform_4'], f"Expected transforms ['add_prefix', 'transform_4'], got {sorted([t['componentId'] for t in info.get('transforms', [])])}"
        assert [s['componentId'] for s in info.get('sinks', [])] == [], f"Expected sinks [], got {[s['componentId'] for s in info.get('sinks', [])]}"

    def test_get_info_add_prefix(self):
        info = self.get_component_info('add_prefix')
        print(f"Info for add_prefix: {json.dumps(info, indent=2)}")
        assert info['componentId'] == 'add_prefix', f"Expected componentId 'add_prefix', got {info['componentId']}"
        assert info['componentType'] == 'remap', f"Expected componentType 'remap', got {info['componentType']}"
        assert sorted(info['inputs']) == ['my_http_source'], f"Expected inputs ['my_http_source'], got {sorted(info['inputs'])}"
        assert sorted(info['outputs']) == ['replace_via'], f"Expected outputs ['replace_via'], got {sorted(info['outputs'])}"
        assert [s['componentId'] for s in info.get('sources', [])] == ['my_http_source'], f"Expected sources ['my_http_source'], got {[s['componentId'] for s in info.get('sources', [])]}"
        assert sorted([t['componentId'] for t in info.get('transforms', [])]) == ['replace_via'], f"Expected transforms ['replace_via'], got {sorted([t['componentId'] for t in info.get('transforms', [])])}"
        assert [s['componentId'] for s in info.get('sinks', [])] == [], f"Expected sinks [], got {[s['componentId'] for s in info.get('sinks', [])]}"

    def test_get_info_replace_via(self):
        info = self.get_component_info('replace_via')
        print(f"Info for replace_via: {json.dumps(info, indent=2)}")
        assert info['componentId'] == 'replace_via', f"Expected componentId 'replace_via', got {info['componentId']}"
        assert info['componentType'] == 'remap', f"Expected componentType 'remap', got {info['componentType']}"
        assert sorted(info['inputs']) == ['add_prefix'], f"Expected inputs ['add_prefix'], got {sorted(info['inputs'])}"
        assert sorted(info['outputs']) == ['uppercase_message'], f"Expected outputs ['uppercase_message'], got {sorted(info['outputs'])}"
        assert [s['componentId'] for s in info.get('sources', [])] == [], f"Expected sources [], got {[s['componentId'] for s in info.get('sources', [])]}"
        assert sorted([t['componentId'] for t in info.get('transforms', [])]) == ['add_prefix', 'uppercase_message'], f"Expected transforms ['add_prefix', 'uppercase_message'], got {sorted([t['componentId'] for t in info.get('transforms', [])])}"
        assert [s['componentId'] for s in info.get('sinks', [])] == [], f"Expected sinks [], got {[s['componentId'] for s in info.get('sinks', [])]}"

    def test_get_info_uppercase_message(self):
        info = self.get_component_info('uppercase_message')
        print(f"Info for uppercase_message: {json.dumps(info, indent=2)}")
        assert info['componentId'] == 'uppercase_message', f"Expected componentId 'uppercase_message', got {info['componentId']}"
        assert info['componentType'] == 'remap', f"Expected componentType 'remap', got {info['componentType']}"
        assert sorted(info['inputs']) == ['replace_via'], f"Expected inputs ['replace_via'], got {sorted(info['inputs'])}"
        assert sorted(info['outputs']) == ['my_console_sink'], f"Expected outputs ['my_console_sink'], got {sorted(info['outputs'])}"
        assert [s['componentId'] for s in info.get('sources', [])] == [], f"Expected sources [], got {[s['componentId'] for s in info.get('sources', [])]}"
        assert sorted([t['componentId'] for t in info.get('transforms', [])]) == ['replace_via'], f"Expected transforms ['replace_via'], got {sorted([t['componentId'] for t in info.get('transforms', [])])}"
        assert [s['componentId'] for s in info.get('sinks', [])] == ['my_console_sink'], f"Expected sinks ['my_console_sink'], got {[s['componentId'] for s in info.get('sinks', [])]}"

    def test_get_info_my_console_sink(self):
        info = self.get_component_info('my_console_sink')
        print(f"Info for my_console_sink: {json.dumps(info, indent=2)}")
        assert info['componentId'] == 'my_console_sink', f"Expected componentId 'my_console_sink', got {info['componentId']}"
        assert info['componentType'] == 'console', f"Expected componentType 'console', got {info['componentType']}"
        assert sorted(info['inputs']) == ['uppercase_message'], f"Expected inputs ['uppercase_message'], got {sorted(info['inputs'])}"
        assert sorted(info['outputs']) == [], f"Expected outputs [], got {sorted(info['outputs'])}"
        assert [s['componentId'] for s in info.get('sources', [])] == [], f"Expected sources [], got {[s['componentId'] for s in info.get('sources', [])]}"
        assert sorted([t['componentId'] for t in info.get('transforms', [])]) == ['uppercase_message'], f"Expected transforms ['uppercase_message'], got {sorted([t['componentId'] for t in info.get('transforms', [])])}"

    def test_subscribe_source(self):
        result = subprocess.run(['python', 'vector_script.py', 'subscribe', '--patterns', 'my_http_source', '--limit', '1'], capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise Exception(f"Subscribe to source failed: {result.stderr}")
        log = result.stdout
        print(f"Subscription log for source: {log}")
        assert 'Subscription sent' in log, "Subscription not sent"
        assert "{ \"event\":" in log, "No events received"
        response = requests.post('http://192.168.1.197:8080', json={'log': log}, headers={'Content-Type': 'application/json'})
        assert response.status_code == 200, f"Post failed: {response.text}"

    def test_subscribe_transform(self):
        process = subprocess.Popen(['python', 'vector_script.py', 'subscribe', '--patterns', 'replace_via', '--limit', '2'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(2)  # Wait for subscription to start
        trigger_url = 'http://192.168.1.197:8080'
        sent_message = '[{"message": "Test POST log via JSON", "level": "info"}]'
        requests.post(trigger_url, data=sent_message, headers={'Content-Type': 'application/json'})
        time.sleep(2)  # Wait for events to be processed
        process.terminate()
        stdout, stderr = process.communicate()
        if process.returncode not in [0, -15]:  # Allow for termination
            raise Exception(f"Subscribe to transform failed: {stderr}")
        log = stdout.split('\n')
        if len(log)>7:
            log = '\n'.join(log[7:len(log)-4])
        else:
            ''.join(parts)
        
        print(f"{log}")
        print("--- EOL ---")
        assert re.search(r'Processed: Test POST', log), "No event with expected json value"
        response = requests.post('http://192.168.1.197:8080', json={'log': log}, headers={'Content-Type': 'application/json'})
        assert response.status_code == 200, f"Post failed: {response.text}"

    def test_subscribe_sink(self):
        result = subprocess.run(['python', 'vector_script.py', 'subscribe', '--patterns', 'my_console_sink', '--limit', '1'], capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise Exception(f"Subscribe to sink failed: {result.stderr}")
        log = result.stdout
        print(f"Subscription log for sink: {log}")
        assert 'Subscription sent' in log, "Subscription not sent"
        assert "{ \"event\":" in log, "No events received"
        response = requests.post('http://192.168.1.197:8080', json={'log': log}, headers={'Content-Type': 'application/json'})
        assert response.status_code == 200, f"Post failed: {response.text}"

    def test_get_chain(self):
        """
        Verifies that the outputs, inputs, sources, transforms, and sinks in the chain JSON
        are consistent and correctly reference each other.
        Returns a tuple (is_valid, message) where is_valid is a boolean and message is a string
        describing any issues found or confirming validity.

        Based on this vector config
            data_dir: /opt/vector/data

            api:
            enabled: true
            address: "0.0.0.0:8686"

            sources:
            my_http_source:
                type: http
                address: 0.0.0.0:8080 # The address and port to listen on
                encoding: json # The expected encoding of the incoming data (e.g., json, text)

            transforms:
            add_prefix:
                type: remap
                inputs:
                - my_http_source
                source: |  # VRL program: add a prefix to the message field
                .message, err = "Processed: " + .message

            replace_via:
                type: remap
                inputs:
                - add_prefix
                source: |  # VRL program: replace "via" with "through" in the message field
                .message, err = replace(.message, "via", "through")

            uppercase_message:
                type: remap
                inputs:
                - replace_via
                source: |  # VRL program: convert the message to uppercase
                .message, err = upcase(.message)

            transform_4:
                type: remap
                inputs:
                - my_http_source
                source: |
                .message, err = "Direct: " + .message

            sinks:
            my_console_sink:
                type: console
                inputs:
                - uppercase_message
                encoding:
                codec: json

        """
        result = subprocess.run(['python', 'vector_script.py', 'get-chain', 'replace_via'], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Get chain failed: {result.stderr}")
        chain = json.loads(result.stdout)
        

        assert len(chain) == 5
        assert sorted(chain.keys()) == ['add_prefix', 'my_console_sink', 'my_http_source', 'replace_via', 'uppercase_message']


        
        # Expected JSON structure for comparison
        expected_chain = {
            "add_prefix": {
                "componentId": "add_prefix",
                "componentType": "remap",
                "outputs": ["replace_via"],
                "sources": [{"componentId": "my_http_source", "componentType": "http"}],
                "transforms": [{"componentId": "replace_via", "componentType": "remap"}],
                "sinks": [],
                "inputs": ["my_http_source"]
            },
            "my_console_sink": {
                "componentId": "my_console_sink",
                "componentType": "console",
                "sources": [],
                "transforms": [{"componentId": "uppercase_message", "componentType": "remap"}],
                "inputs": ["uppercase_message"],
                "outputs": []
            },
            "my_http_source": {
                "componentId": "my_http_source",
                "componentType": "http",
                "outputTypes": ["LOG"],
                "outputs": ["add_prefix"],
                "transforms": [{"componentId": "add_prefix", "componentType": "remap"}],
                "sinks": [],
                "inputs": []
            },
            "replace_via": {
                "componentId": "replace_via",
                "componentType": "remap",
                "outputs": ["uppercase_message"],
                "sources": [],
                "transforms": [
                    {"componentId": "add_prefix", "componentType": "remap"},
                    {"componentId": "uppercase_message", "componentType": "remap"}
                ],
                "sinks": [],
                "inputs": ["add_prefix"]
            },
            "uppercase_message": {
                "componentId": "uppercase_message",
                "componentType": "remap",
                "outputs": ["my_console_sink"],
                "sources": [],
                "transforms": [{"componentId": "replace_via", "componentType": "remap"}],
                "sinks": [{"componentId": "my_console_sink", "componentType": "console"}],
                "inputs": ["replace_via"]
            }
        }

        # Check if all expected components are present
        expected_keys = set(expected_chain.keys())
        actual_keys = set(chain.keys())
        if expected_keys != actual_keys:
            return False, f"Component mismatch: Expected {expected_keys}, got {actual_keys}"

        # Verify each component's fields
        for component_id, expected_component in expected_chain.items():
            actual_component = chain.get(component_id)
            if not actual_component:
                return False, f"Component {component_id} missing in chain"

            # Check required fields
            required_fields = ["componentId", "componentType", "outputs", "sources", "transforms", "inputs"]
            if component_id == "my_console_sink":
                required_fields.append("sinks")  # sinks is optional except for my_console_sink and uppercase_message
            if component_id == "uppercase_message":
                required_fields.append("sinks")
            if component_id == "my_http_source":
                required_fields.append("outputTypes")

            for field in required_fields:
                if field not in actual_component:
                    return False, f"Field {field} missing in component {component_id}"

            # Verify field values match exactly (excluding metrics)
            for field in required_fields:
                expected_value = expected_component.get(field)
                actual_value = actual_component.get(field)
                if expected_value != actual_value:
                    return False, f"Mismatch in {component_id}.{field}: Expected {expected_value}, got {actual_value}"

            # Verify component references in sources, transforms, and sinks
            for field in ["sources", "transforms", "sinks"]:
                if field not in actual_component:
                    continue
                for ref in actual_component[field]:
                    ref_id = ref.get("componentId")
                    ref_type = ref.get("componentType")
                    if ref_id not in chain:
                        return False, f"Invalid reference in {component_id}.{field}: {ref_id} does not exist"
                    if chain[ref_id]["componentType"] != ref_type:
                        return False, f"Type mismatch in {component_id}.{field}: {ref_id} has type {chain[ref_id]['componentType']}, expected {ref_type}"

            # Verify cross-references for inputs and outputs
            for input_id in actual_component["inputs"]:
                if input_id not in chain:
                    return False, f"Invalid input in {component_id}: {input_id} does not exist"
                if component_id not in chain[input_id]["outputs"]:
                    return False, f"Input mismatch: {component_id} lists {input_id} as input, but {input_id} does not list {component_id} in outputs"

            for output_id in actual_component["outputs"]:
                if output_id not in chain:
                    return False, f"Invalid output in {component_id}: {output_id} does not exist"
                if component_id not in chain[output_id]["inputs"]:
                    return False, f"Output mismatch: {component_id} lists {output_id} as output, but {output_id} does not list {component_id} in inputs"

        # Verify dataflow consistency (optional, but ensures pipeline integrity)
        pipeline_order = ["my_http_source", "add_prefix", "replace_via", "uppercase_message", "my_console_sink"]
        for i in range(len(pipeline_order) - 1):
            current = pipeline_order[i]
            next_comp = pipeline_order[i + 1]
            if next_comp not in chain[current]["outputs"]:
                return False, f"Pipeline broken: {current} does not output to {next_comp}"

        return True, "Chain JSON is valid and matches the expected structure"

if __name__ == '__main__':
    nose2.main()
